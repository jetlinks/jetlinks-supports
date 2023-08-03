package org.jetlinks.supports.config;

import com.google.common.collect.Maps;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.config.ConfigKey;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.utils.Reactors;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import static org.jetlinks.supports.config.EventBusStorageManager.NOTIFY_TOPIC;

public class LocalCacheClusterConfigStorage implements ConfigStorage {
    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<Cache, Mono> CACHE_REF = AtomicReferenceFieldUpdater
            .newUpdater(Cache.class, Mono.class, "ref");

    private static final AtomicIntegerFieldUpdater<Cache> CACHE_VERSION = AtomicIntegerFieldUpdater
            .newUpdater(Cache.class, "version");

    private static final AtomicReferenceFieldUpdater<Cache, Disposable> CACHE_LOADER = AtomicReferenceFieldUpdater
            .newUpdater(Cache.class, Disposable.class, "loader");

    private static final Map<Value, Value> shared = new ConcurrentReferenceHashMap<>(1024);

    private static final Set<String> sharedKey = ConcurrentHashMap.newKeySet();

    private final Map<String, Cache> caches;
    private final String id;
    private final EventBus eventBus;

    private final ClusterCache<String, Object> clusterCache;

    private final long expires;
    private final Runnable doOnClear;

    public static final Value NULL = Value.simple(null);

    public LocalCacheClusterConfigStorage(String id,
                                          EventBus eventBus,
                                          ClusterCache<String, Object> clusterCache,
                                          long expires,
                                          Runnable doOnClear,
                                          Map<String, Cache> cacheContainer) {
        this.id = id;
        this.eventBus = eventBus;
        this.clusterCache = clusterCache;
        this.expires = expires;
        this.doOnClear = doOnClear;
        this.caches = cacheContainer;
    }

    public LocalCacheClusterConfigStorage(String id, EventBus eventBus,
                                          ClusterCache<String, Object> clusterCache,
                                          long expires,
                                          Runnable doOnClear) {
        this(id, eventBus, clusterCache, expires, doOnClear, new ConcurrentHashMap<>());
    }

    private static Value tryShare(String key, Value val) {
        if (!sharedKey.contains(key)) {
            return val;
        }
        return shared.computeIfAbsent(val, Function.identity());
    }

    public class Cache {
        final String key;
        long t;
        volatile int version;
        volatile Value cached;

        volatile Mono<Value> ref;
        Sinks.One<Value> sink;
        volatile Disposable loader;

        public Cache(String key) {
            this.key = key;
            updateTime();
        }

        boolean isExpired() {
            return expires > 0 && System.currentTimeMillis() - t > expires;
        }

        Mono<Value> getRef() {
            @SuppressWarnings("unchecked")
            Mono<Value> ref = CACHE_REF.get(this);
            if (isExpired() || ref == null) {
                return reload();
            }
            return ref;
        }

        public Value getCached() {
            if (isExpired()) {
                return null;
            }
            return cached;
        }

        public Object getCachedValue() {
            Value cached = getCached();
            return cached == null ? null : cached.get();
        }

        void updateTime() {
            if (expires > 0) {
                t = System.currentTimeMillis();
            }
        }

        void setValue(Object value) {
            setValue(value == null ? null : Value.simple(value));
        }

        void setValue(Value value) {
            updateTime();

            CACHE_VERSION.incrementAndGet(this);
            if (value != null) {
                value = tryShare(key, value);
                this.ref = Mono.just(value);
                this.cached = value;
            } else {
                this.ref = Mono.empty();
                this.cached = NULL;
            }

            dispose();
        }

        synchronized Mono<Value> reload() {
            cached = null;
            dispose();

            int version = this.version;
            sink = Sinks.one();
            Mono<Value> ref = sink.asMono();
            CACHE_REF.set(this, ref);

            Disposable[] disposables = new Disposable[1];

            CACHE_LOADER.set(
                    this,
                    disposables[0] = clusterCache
                            .get(key)
                            .switchIfEmpty(Mono.fromRunnable(() -> {
                                if (version == this.version) {
                                    this.setValue(null);
                                } else {
                                    this.clear();
                                }
                                CACHE_LOADER.compareAndSet(this, disposables[0], null);
                            }))
                            .subscribe(
                                    value -> {
                                        if (this.version == version) {
                                            this.setValue(value);
                                        } else {
                                            this.clear();
                                        }
                                        CACHE_LOADER.compareAndSet(this, disposables[0], null);
                                    },
                                    err -> {
                                        this.clear();
                                        sink.tryEmitError(err);
                                        CACHE_LOADER.compareAndSet(this, disposables[0], null);
                                    })
            );
            return ref;
        }

        void clear() {
            dispose();
            cached = null;
            CACHE_VERSION.incrementAndGet(this);
            CACHE_REF.set(this, null);
        }

        void dispose() {
            Disposable disposable = CACHE_LOADER.getAndSet(this, null);
            if (null != disposable) {
                disposable.dispose();
            }

            Sinks.One<Value> sink = this.sink;
            this.sink = null;

            if (sink != null) {
                Value value = this.cached != null ? this.cached : null;
                if (value == null || value.get() == null) {
                    sink.tryEmitEmpty();
                } else {
                    sink.tryEmitValue(value);
                }
            }
        }

    }

    private Cache createCache(String key) {
        return new Cache(key);
    }

    private Cache getOrCreateCache(String key) {
        return caches
                .computeIfAbsent(key, this::createCache);
    }

    @Override
    public Mono<Value> getConfig(String key) {
        return getOrCreateCache(key).getRef();
    }

    @Override
    public Mono<Values> getConfigs(Collection<String> keys) {
        int hits = 0;
        Map<String, Object> loaded = Maps.newHashMapWithExpectedSize(keys.size());
        //获取一级缓存
        for (String key : keys) {
            Cache local = getOrCreateCache(key);
            Value cached = local.getCached();
            if (cached != null) {
                //命中一级缓存
                hits++;
                loaded.put(key, cached.get());
            }
        }
        Values wrap = Values.of(Maps.filterValues(loaded, Objects::nonNull));
        //全部来自一级缓存则直接返回
        if (hits == keys.size()) {
            return Mono.just(wrap);
        }

        //需要从二级缓存中加载的配置
        Set<String> needLoadKeys = new HashSet<>(keys);
        needLoadKeys.removeAll(loaded.keySet());
        if (needLoadKeys.isEmpty()) {
            return Mono.just(wrap);
        }

        //当前版本信息
        Map<String, Integer> versions = Maps.newHashMapWithExpectedSize(needLoadKeys.size());
        for (String needLoadKey : needLoadKeys) {
            Cache cache = caches.get(needLoadKey);
            if (cache != null) {
                versions.put(needLoadKey, cache.version);
            }
        }

        return clusterCache
                .get(needLoadKeys)
                .reduce(loaded, (map, entry) -> {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    //加载到一级缓存中
                    Cache cache = getOrCreateCache(key);
                    int version = versions.getOrDefault(key, cache.version);
                    updateValue(cache, version, value);
                    if (null != value) {
                        map.put(key, value);
                    }
                    return map;
                })
                .defaultIfEmpty(Collections.emptyMap())
                .doOnNext(map -> {
                    needLoadKeys.removeAll(map.keySet());
                    //还有配置没加载出来,说明这些配置不存在，则全部设置为null
                    if (needLoadKeys.size() > 0) {
                        for (String needLoadKey : needLoadKeys) {
                            Cache cache = this.getOrCreateCache(needLoadKey);
                            int version = versions.getOrDefault(needLoadKey, cache.version);
                            updateValue(cache, version, null);
                        }
                    }
                })
                .thenReturn(wrap);
    }

    private void updateValue(Cache cache, int version, Object value) {
        if (cache != null && cache.version == version) {
            cache.setValue(value);
        }
    }

    @Override
    public Mono<Boolean> setConfigs(Map<String, Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return Reactors.ALWAYS_TRUE;
        }
        values.forEach((key, value) -> getOrCreateCache(key).setValue(value));

        return clusterCache
                .putAll(values)
                .then(notify(CacheNotify.expires(id, values.keySet())))
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> setConfig(String key, Object value) {
        if (key == null) {
            return Reactors.ALWAYS_FALSE;
        }
        if (value == null) {
            return remove(key);
        }
        getOrCreateCache(key).setValue(value);

        return clusterCache
                .put(key, value)
                .then(notifyRemoveKey(key))
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> remove(String key) {
        return clusterCache
                .remove(key)
                .then(notifyRemoveKey(key))
                .thenReturn(true);
    }

    @Override
    public Mono<Value> getAndRemove(String key) {
        return clusterCache
                .getAndRemove(key)
                .flatMap(res -> notify(CacheNotify.expires(id, Collections.singleton(key))).thenReturn(res))
                .map(Value::simple);
    }

    @Override
    public Mono<Boolean> remove(Collection<String> key) {
        return clusterCache
                .remove(key)
                .then(notify(CacheNotify.expires(id, key)))
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> clear() {
        return clusterCache
                .clear()
                .then(notify(CacheNotify.clear(id)))
                .thenReturn(true);
    }

    void clearLocalCache(CacheNotify notify) {
        if (CollectionUtils.isEmpty(notify.getKeys())) {
            caches.clear();
        } else {
            notify.getKeys().forEach(caches::remove);
        }
        if (notify.isClear()) {
            if (doOnClear != null) {
                doOnClear.run();
            }
        }
    }

    Mono<Void> notify(CacheNotify notify) {
        clearLocalCache(notify);
        return eventBus
                .publish(NOTIFY_TOPIC, notify)
                .then();
    }

    Mono<Void> notifyRemoveKey(String key) {
        return notify(CacheNotify.expires(id, Collections.singleton(key)));
    }

    @Override
    public Mono<Void> refresh() {
        return notify(CacheNotify.expiresAll(id));
    }

    @Override
    public Mono<Void> refresh(Collection<String> keys) {
        return notify(CacheNotify.expires(id, keys));
    }

    static {

        addSharedKey(DeviceConfigKey.productId);
        addSharedKey(DeviceConfigKey.protocol);
        addSharedKey(DeviceConfigKey.connectionServerId);
        addSharedKey("state");
        addSharedKey("productName");

    }

    public static void addSharedKey(ConfigKey<?>... key) {
        for (ConfigKey<?> configKey : key) {
            sharedKey.add(configKey.getKey());
        }
    }

    public static void addSharedKey(String... key) {
        sharedKey.addAll(Arrays.asList(key));
    }
}
