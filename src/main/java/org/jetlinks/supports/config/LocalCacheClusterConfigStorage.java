package org.jetlinks.supports.config;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.config.ConfigKey;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.utils.Reactors;
import org.reactivestreams.Subscription;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.util.context.Context;

import javax.annotation.Nonnull;
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


    private Cache createCache(String key) {
        return new Cache(key);
    }

    private Cache getOrCreateCache(String key) {
        return caches
            .computeIfAbsent(key, this::createCache);
    }

    @Override
    public Mono<Value> getConfig(String key) {
        return getConfig(key, Mono.empty());
    }

    @Override
    public Mono<Value> getConfig(String key, Mono<Object> loader) {
        return getOrCreateCache(key)
            .getRef(loader);
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

        return new MultiCacheLoaderMono(
            clusterCache.get(needLoadKeys),
            needLoadKeys,
            versions,
            loaded,
            wrap
        );

//        return clusterCache
//            .get(needLoadKeys)
//            .reduce(loaded, (map, entry) -> {
//                String key = entry.getKey();
//                Object value = entry.getValue();
//                //加载到一级缓存中
//                Cache cache = getOrCreateCache(key);
//                int version = versions.getOrDefault(key, cache.version);
//                updateValue(cache, version, value);
//                if (null != value) {
//                    map.put(key, value);
//                }
//                return map;
//            })
//            .defaultIfEmpty(Collections.emptyMap())
//            .doOnNext(map -> {
//                needLoadKeys.removeAll(map.keySet());
//                //还有配置没加载出来,说明这些配置不存在，则全部设置为null
//                if (!needLoadKeys.isEmpty()) {
//                    for (String needLoadKey : needLoadKeys) {
//                        Cache cache = this.getOrCreateCache(needLoadKey);
//                        int version = versions.getOrDefault(needLoadKey, cache.version);
//                        updateValue(cache, version, null);
//                    }
//                }
//            })
//            .thenReturn(wrap);
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
            .then(Reactors.ALWAYS_TRUE);
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
            .then(Reactors.ALWAYS_TRUE);
    }

    @Override
    public Mono<Boolean> remove(String key) {
        return clusterCache
            .remove(key)
            .then(notifyRemoveKey(key))
            .then(Reactors.ALWAYS_TRUE);
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
            .then(Reactors.ALWAYS_TRUE);
    }

    @Override
    public Mono<Boolean> clear() {
        return clusterCache
            .clear()
            .then(notify(CacheNotify.clear(id)))
            .then(Reactors.ALWAYS_TRUE);
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

    private static Value tryShare(String key, Value val) {
        if (!sharedKey.contains(key)) {
            return val;
        }
        return shared.computeIfAbsent(val, Function.identity());
    }

    @AllArgsConstructor
    class MultiCacheLoaderMono extends Mono<Values> {
        private final Flux<? extends Map.Entry<String, Object>> source;
        private final Set<String> keys;
        private final Map<String, Integer> versions;
        private final Map<String, Object> container;
        private final Values wrapper;

        @Override
        public void subscribe(@Nonnull CoreSubscriber<? super Values> actual) {
            source.subscribe(
                new MultiCacheLoader(keys, versions, container, actual, wrapper)
            );
        }

    }

    @AllArgsConstructor
    class MultiCacheLoader extends BaseSubscriber<Map.Entry<String, Object>> {
        private final Set<String> keys;
        private final Map<String, Integer> versions;
        private final Map<String, Object> container;
        private final CoreSubscriber<? super Values> actual;
        private final Values wrapper;

        @Override
        protected void hookOnSubscribe(@Nonnull Subscription subscription) {
            actual.onSubscribe(this);
        }

        @Override
        @Nonnull
        public Context currentContext() {
            return actual.currentContext();
        }

        @Override
        protected void hookOnNext(Map.Entry<String, Object> entry) {
            String key = entry.getKey();
            Object value = entry.getValue();
            //加载到一级缓存中
            Cache cache = getOrCreateCache(key);
            int version = versions.getOrDefault(key, cache.version);
            updateValue(cache, version, value);
            //设置最新的值,防止并发更新导致不一致
            container.put(key, cache.getCachedValue());
            if (null != value) {
                keys.remove(key);
            }
        }

        @Override
        protected void hookOnError(@Nonnull Throwable throwable) {
            actual.onError(throwable);
        }

        @Override
        protected void hookOnComplete() {
            //还有配置没加载出来,说明这些配置不存在，则全部设置为null
            if (!keys.isEmpty()) {
                for (String needLoadKey : keys) {
                    Cache cache = getOrCreateCache(needLoadKey);
                    int version = versions.getOrDefault(needLoadKey, cache.version);
                    updateValue(cache, version, null);
                    //设置最新的值,防止并发更新导致不一致
                    container.put(needLoadKey, cache.getCachedValue());
                }
            }
            actual.onNext(wrapper);
            actual.onComplete();
        }
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

        @SuppressWarnings("unchecked")
        Mono<Value> getRef(Mono<Object> loader) {
            Mono<Value> ref = CACHE_REF.get(this);
            if (isExpired() || ref == null) {
                synchronized (this) {
                    ref = CACHE_REF.get(this);
                    if (ref == null) {
                        return reload(loader);
                    } else {
                        return ref;
                    }
                }
            }
            return ref;
        }

        public Value getCached() {
            if (isExpired()) {
                return null;
            }
            updateTime();
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

        Mono<Value> reload(Mono<Object> loader) {
            cached = null;
            dispose();
            this.sink = Sinks.one();
            Mono<Value> ref = sink.asMono();
            CACHE_REF.set(this, ref);
            Loader _loader = new Loader(this.version);
            CACHE_LOADER.set(this, _loader);

            clusterCache
                .get(key)
                .switchIfEmpty(loader)
                .subscribe(_loader);

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
                    sink.emitEmpty(Reactors.emitFailureHandler());
                } else {
                    sink.emitValue(value, Reactors.emitFailureHandler());
                }
            }
        }

        @AllArgsConstructor
        private class Loader extends BaseSubscriber<Object> {
            private final long loadingVersion;

            @Override
            protected void hookOnNext(@Nonnull Object value) {
                if (loadingVersion == version) {
                    setValue(value);
                } else {
                    clear();
                }
            }

            @Override
            protected void hookOnComplete() {
                if (sink != null && loadingVersion == version) {
                    setValue(null);
                }
            }

            @Override
            protected void hookOnError(@Nonnull Throwable throwable) {
                Sinks.One<Value> _sink = sink;
                if (_sink != null) {
                    Value cached = getCached();
                    if (cached != null && cached.get() != null) {
                        _sink.emitValue(cached, Reactors.emitFailureHandler());
                    } else {
                        _sink.emitError(throwable, Reactors.emitFailureHandler());
                    }
                }
                clear();
            }

            @Override
            protected void hookFinally(@Nonnull SignalType type) {
                CACHE_LOADER.compareAndSet(Cache.this, this, null);
            }
        }
    }
}
