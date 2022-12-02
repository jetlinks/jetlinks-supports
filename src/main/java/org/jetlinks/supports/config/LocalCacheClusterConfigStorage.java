package org.jetlinks.supports.config;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.utils.Reactors;
import org.springframework.util.CollectionUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.jetlinks.supports.config.EventBusStorageManager.NOTIFY_TOPIC;

@AllArgsConstructor
public class LocalCacheClusterConfigStorage implements ConfigStorage {
    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<Cache, Mono> CACHE_REF = AtomicReferenceFieldUpdater
            .newUpdater(Cache.class, Mono.class, "ref");

    private static final AtomicIntegerFieldUpdater<Cache> CACHE_VERSION = AtomicIntegerFieldUpdater
            .newUpdater(Cache.class, "version");

    private static final AtomicReferenceFieldUpdater<Cache, Disposable> CACHE_LOADER = AtomicReferenceFieldUpdater
            .newUpdater(Cache.class, Disposable.class, "loader");

    private final Map<String, Cache> caches = new NonBlockingHashMap<>();
    private final String id;
    private final EventBus eventBus;

    private final ClusterCache<String, Object> clusterCache;

    private long expires;
    private final Runnable doOnClear;

    public static final Value NULL = Value.simple(null);

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
            if (isExpired() || ref == null) {
                reload();
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
            this.ref = Mono.justOrEmpty(value);
            this.cached = value == null ? NULL : value;

            dispose();
        }

        synchronized void reload() {
            cached = null;
            dispose();

            int version = this.version;
            sink = Sinks.one();
            CACHE_REF.set(this, sink.asMono());

            loader = clusterCache
                    .get(key)
                    .switchIfEmpty(Mono.fromRunnable(() -> {
                        if (version == this.version) {
                            this.setValue(null);
                        } else {
                            this.clear();
                        }
                    }))
                    .subscribe(
                            value -> {
                                if (this.version == version) {
                                    this.setValue(value);
                                } else {
                                    this.clear();
                                }
                            },
                            err -> {
                                this.clear();
                                sink.tryEmitError(err);
                            });
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

    Values wrapCache(Collection<String> keys) {
        return Values
                .of(Maps.filterEntries(
                        Maps.transformValues(caches, Cache::getCachedValue),
                        e -> e.getValue() != null && keys.contains(e.getKey())
                ));
    }

    @Override
    public Mono<Values> getConfigs(Collection<String> keys) {
        int hits = 0;
        //获取一级缓存
        for (String key : keys) {
            Cache local = getOrCreateCache(key);
            Value cached = local.getCached();
            if (cached != null) {
                //命中一级缓存
                hits++;
            }
        }
        //全部来自一级缓存则直接返回
        if (hits == keys.size()) {
            return Mono.just(wrapCache(keys));
        }
        Values wrap = wrapCache(keys);
        //需要从二级缓存中加载的配置
        Set<String> needLoadKeys = new HashSet<>(keys);
        needLoadKeys.removeAll(wrap.getAllValues().keySet());

        if (needLoadKeys.isEmpty()) {
            return Mono.just(wrap);
        }

        //当前版本信息
        Map<String, Integer> versions = Maps.newHashMapWithExpectedSize(caches.size());
        for (Map.Entry<String, Cache> entry : caches.entrySet()) {
            versions.put(entry.getKey(), entry.getValue().version);
        }

        return clusterCache
                .get(needLoadKeys)
                .<Map<String, Cache>>reduce(new HashMap<>(), (map, entry) -> {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    //加载到一级缓存中
                    Cache cache = getOrCreateCache(key);
                    int version = versions.getOrDefault(key, cache.version);
                    updateValue(cache, version, value);
                    if (null != value) {
                        map.put(key, cache);
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
}
