package org.jetlinks.supports.config;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.exception.BusinessException;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.jetlinks.supports.config.EventBusStorageManager.NOTIFY_TOPIC;

@Slf4j
public class LocalCacheClusterConfigStorage implements ConfigStorage {
    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<Cache, Mono> CACHE_REF = AtomicReferenceFieldUpdater
        .newUpdater(Cache.class, Mono.class, "ref");
    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<Cache, Value> CACHED = AtomicReferenceFieldUpdater
        .newUpdater(Cache.class, Value.class, "cached");

    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<Cache, Sinks.One> AWAIT = AtomicReferenceFieldUpdater
        .newUpdater(Cache.class, Sinks.One.class, "await");

    private static final AtomicIntegerFieldUpdater<Cache> CACHE_VERSION = AtomicIntegerFieldUpdater
        .newUpdater(Cache.class, "version");

    private static final AtomicReferenceFieldUpdater<Cache, Disposable> CACHE_LOADING = AtomicReferenceFieldUpdater
        .newUpdater(Cache.class, Disposable.class, "loading");

    private static final Map<Value, Value> shared = new ConcurrentReferenceHashMap<>(1024);

    private static final Set<String> sharedKey = ConcurrentHashMap.newKeySet();

    public static final Value NULL = Value.simple(null);

    private final Map<String, Cache> caches;
    final String id;
    private final EventBus eventBus;

    private final ClusterCache<String, Object> clusterCache;

    private final long expires;
    private final Consumer<LocalCacheClusterConfigStorage> doOnClear;

    Throwable lastError;

    public LocalCacheClusterConfigStorage(String id,
                                          EventBus eventBus,
                                          ClusterCache<String, Object> clusterCache,
                                          long expires,
                                          Consumer<LocalCacheClusterConfigStorage> doOnClear,
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
                                          Consumer<LocalCacheClusterConfigStorage> doOnClear) {
        this(id, eventBus, clusterCache, expires, doOnClear, new ConcurrentHashMap<>());
    }


    boolean isEmpty() {
        return caches.isEmpty();
    }

    void cleanup() {
        if (expires > 0) {
            caches.forEach((k, v) -> {
                if (v.isExpired()) {
                    caches.remove(k, v);
                }
            });
        }
    }

    void error(Throwable err) {
        this.lastError = err;
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

    }

    private void updateValue(Cache cache, int version, Object value) {
        if (cache != null && CACHE_VERSION.get(cache) == version) {
            cache.setValue(version, value);
        }
    }

    @Override
    public Mono<Boolean> setConfigs(Map<String, Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return Reactors.ALWAYS_TRUE;
        }
        values.forEach(this::trySetLocalValue);

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
        trySetLocalValue(key, value);
        return clusterCache
            .put(key, value)
            .then(notifyRemoveKey(key))
            .then(Reactors.ALWAYS_TRUE);
    }

    private void trySetLocalValue(String key, Object value) {
        if (value == null) {
            return;
        }
        Cache cache = caches.get(key);
        if (cache != null && cache.loading == null) {
            cache.setValue(cache.version, value);
        }
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
                doOnClear.accept(this);
            }
        }
    }

    Mono<Void> notify(CacheNotify notify) {
        return Mono
            .defer(() -> {
                clearLocalCache(notify);
                return eventBus.publish(NOTIFY_TOPIC, notify);
            })
            .then();
    }

    Mono<Void> notifyRemoveKey(String key) {
        return notify(CacheNotify.expires(id, Collections.singleton(key)));
    }

    Map<String, Object> getAll() {
        return Collections.unmodifiableMap(
            Maps.transformValues(caches, Cache::getCachedValue)
        );
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
            Object val = cache.getCachedValue();
            //最新值为null?可能是并发读写.版本不一致,还未成功加载值,直接使用获取到的值.
            if (val == null) {
                val = value;
            }
            //设置最新的值,防止并发更新导致不一致
            container.put(key, val);
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
        volatile Sinks.One<Value> await;
        volatile Disposable loading;

        @Override
        public String toString() {
            return key + ":" + version + "(" + cached + ")";
        }

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
                return reload(loader);
            }
            updateTime();
            Value cached = this.cached;
            if (cached != null && !(ref instanceof Callable)) {
                // 补偿，在某些极端情况下，await没有正确清理？
                if (CACHE_LOADING.get(this) == null && AWAIT.get(this) != null) {
                    error(new BusinessException("conflict cache load"));
                }
                return cached.get() == null ? Mono.empty() : Mono.just(cached);
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
            updateTime();
            Value cached = this.cached;
            return cached == null ? null : cached.get();
        }

        void updateTime() {
            if (expires > 0) {
                t = System.currentTimeMillis();
            }
        }

        boolean setValue(int version, Object value) {
            return setValue(await(), CACHE_LOADING.get(this), version, value);
        }

        boolean setValue(Sinks.One<Value> await, Disposable loading, int version, Object value) {
            return setValue(await, loading, version, value == null ? null : Value.simple(value));
        }

        boolean updateValue(int version, Value value) {
            Value cached = CACHED.get(this);

            if (CACHE_VERSION.compareAndSet(this, version, version + 1)) {
                if (value != null) {
                    value = tryShare(key, value);
                    if (CACHED.compareAndSet(this, cached, value)) {
                        CACHE_REF.set(this, Mono.just(value));
                    }
                } else {
                    if (CACHED.compareAndSet(this, cached, NULL)) {
                        CACHE_REF.set(this, Mono.empty());
                    }
                }
                return true;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(
                        "local cache [id=`{},`key=`{}` value={}] version not match, expect:{},actual:{}",
                        id, key, value == null ? null : value.get(), version, this.version
                    );
                }
                CACHE_REF.set(this, null);
                return false;
            }
        }

        boolean setValue(Sinks.One<Value> await, Disposable loading, int version, Value value) {
            updateTime();

            boolean success = updateValue(version, value);
            Value cached = CACHED.get(this);

            dispose(await, loading, cached == null ? value : cached);

            return success;
        }

        Mono<Value> reload(Mono<Object> loader) {
            for (; ; ) {
                @SuppressWarnings("all")
                Sinks.One<Value> await = AWAIT.get(this);
                if (await != null) {
                    return await.asMono();
                }
                await = Sinks.one();
                if (AWAIT.compareAndSet(this, null, await)) {
                    @SuppressWarnings("all")
                    Mono<Value> ref = await.asMono();
                    CACHE_REF.set(this, ref);
                    Loader _loader = new Loader(this.version);
                    Disposable old = CACHE_LOADING.getAndSet(this, _loader);
                    if (old != null) {
                        old.dispose();
                    }
                    clusterCache
                        .get(key)
                        .switchIfEmpty(loader)
                        .subscribe(_loader);
                    return ref;
                }

            }
        }

        void clear(Value value) {
            Value val = CACHED.get(this);
            if (val == null) {
                val = value;
            }
            dispose(val);
            //  cached = null;
            CACHE_REF.set(this, null);
            CACHE_VERSION.incrementAndGet(this);
        }

        void dispose(Sinks.One<Value> sink, Disposable loading, Value value) {
            if (sink != null) {
                AWAIT.compareAndSet(this, sink, null);
                if (value == null || value.get() == null) {
                    sink.emitEmpty(Reactors.emitFailureHandler());
                } else {
                    sink.emitValue(value, Reactors.emitFailureHandler());
                }
            }
            if (loading != null) {
                CACHE_LOADING.compareAndSet(this, loading, null);
                loading.dispose();
            }
        }

        void dispose(Value value) {
            dispose(await(), CACHE_LOADING.get(this), value);
        }

        void error(Throwable error) {
            LocalCacheClusterConfigStorage.this.error(error);
            Value cached = CACHED.get(this);
            @SuppressWarnings("all")
            Sinks.One<Value> await = AWAIT.getAndSet(Cache.this, null);
            if (await != null) {
                if (cached != null) {
                    log.warn("load cache [{} {}] failed,use cached value:{} ,ver:{}", id, key, cached, version, error);
                    if (cached.get() == null) {
                        await.emitEmpty(Reactors.emitFailureHandler());
                    } else {
                        await.emitValue(cached, Reactors.emitFailureHandler());
                    }
                } else {
                    await.emitError(error, Reactors.emitFailureHandler());
                }
            }
            clear(cached);
        }

        @SuppressWarnings("all")
        Sinks.One<Value> await() {
            return AWAIT.get(this);
        }

        @RequiredArgsConstructor
        private class Loader extends BaseSubscriber<Object> {
            private final int loadingVersion;
            private boolean hasValue;

            @Override
            protected void hookOnSubscribe(@Nonnull Subscription subscription) {
                log.trace("loading cache `{}` key `{}`", id, key);
                super.hookOnSubscribe(subscription);
            }

            @Override
            protected void hookOnNext(@Nonnull Object value) {
                hasValue = true;
                if (!setValue(await(), this, loadingVersion, value)) {
                    //版本不一致,清空缓存,下次重新加载最新数据.
                    clear(Value.simple(value));
                }
            }

            @Override
            protected void hookOnComplete() {
                Sinks.One<Value> await = await();

                if (!hasValue && await != null && loadingVersion == CACHE_VERSION.get(Cache.this)) {
                    setValue(await, this, loadingVersion, null);
                }
            }

            @Override
            protected void hookOnError(@Nonnull Throwable throwable) {
                Cache.this.error(throwable);
            }

            @Override
            protected void hookFinally(@Nonnull SignalType type) {
                CACHE_LOADING.compareAndSet(Cache.this, this, null);
            }
        }
    }

    @Override
    public String toString() {

        return caches.toString();
    }
}
