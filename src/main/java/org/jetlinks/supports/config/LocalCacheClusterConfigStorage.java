package org.jetlinks.supports.config;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.event.EventBus;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
public class LocalCacheClusterConfigStorage implements ConfigStorage {
    private final Map<String, Cache> caches = new ConcurrentHashMap<>();
    private final String id;
    private final EventBus eventBus;

    private final ClusterCache<String, Object> clusterCache;

    private long expires;

    public static final Value NULL = Value.simple(null);

    class Cache {
        long t = System.currentTimeMillis();
        Mono<Value> dataGetter;
        volatile Mono<Value> ref;
        volatile Value cached;

        boolean isExpired() {
            return expires > 0 && System.currentTimeMillis() - t > expires;
        }

        Mono<Value> getRef() {
            if (isExpired()) {
                return dataGetter;
            }
            return ref;
        }

        public Value getCached() {
            if (isExpired()) {
                return null;
            }
            return cached;
        }

        void setValue(Object value) {
            setValue(value == null ? null : Value.simple(value));
        }

        void setValue(Value value) {
            this.t = System.currentTimeMillis();
            this.ref = Mono.justOrEmpty(value);
            this.cached = value == null ? NULL : value;
        }

        void reload() {
            cached = null;
            ref = this.dataGetter.cache();
        }

        void init(Mono<Value> dataGetter) {
            this.dataGetter = dataGetter
                    .doOnNext(this::setValue)
                    .switchIfEmpty(Mono.fromRunnable(() -> this.setValue(null)));
            reload();
        }
    }

    private Cache createCache(String key) {
        Cache cache = new Cache();
        cache.init(clusterCache.get(key).map(Value::simple));
        return cache;
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
        Map<String, Object> caches = Maps.newHashMapWithExpectedSize(keys.size());
        int hits = 0;
        for (String key : keys) {
            Cache local = getOrCreateCache(key);
            Value cached = local.getCached();
            if (cached != null) {
                hits++;
                Object obj = cached.get();
                if (null != obj) {
                    caches.put(key, obj);
                }
            }
        }
        if (hits == keys.size()) {
            return Mono.just(Values.of(caches));
        }
        Set<String> needLoadKeys = new HashSet<>(keys);
        needLoadKeys.removeAll(caches.keySet());

        return clusterCache
                .get(needLoadKeys)
                .reduce(caches, (map, entry) -> {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    getOrCreateCache(key).setValue(value);
                    if (null != value) {
                        map.put(key, value);
                    }
                    return map;
                })
                .defaultIfEmpty(Collections.emptyMap())
                .doOnNext(map -> {
                    needLoadKeys.removeAll(map.keySet());
                    if (needLoadKeys.size() > 0) {
                        for (String needLoadKey : needLoadKeys) {
                            getOrCreateCache(needLoadKey).setValue(null);
                        }
                    }
                })
                .map(Values::of);
    }

    @Override
    public Mono<Boolean> setConfigs(Map<String, Object> values) {
        if (CollectionUtils.isEmpty(values)) {
            return Mono.just(true);
        }
        values.forEach((key, value) -> getOrCreateCache(key).setValue(value));

        return clusterCache
                .putAll(values)
                .then(notifyRemoveKey("__all"))
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> setConfig(String key, Object value) {
        if (key == null) {
            return Mono.just(false);
        }
        if (value == null) {
            return remove(key);
        }
        getOrCreateCache(key).setValue(value);

        return clusterCache
                .put(key, value)
                .then(notifyRemoveKey(key))
                .thenReturn(true)
                .doOnSuccess(s -> {
                    Cache cache = caches.get(key);
                    if (cache != null) {
                        cache.reload();
                    }
                });
    }

    @Override
    public Mono<Boolean> remove(String key) {
        caches.remove(key);
        return clusterCache
                .remove(key)
                .then(notifyRemoveKey(key))
                .thenReturn(true);
    }

    @Override
    public Mono<Value> getAndRemove(String key) {
        return clusterCache
                .getAndRemove(key)
                .flatMap(res -> notifyRemoveKey("__all").thenReturn(res))
                .map(Value::simple);
    }

    @Override
    public Mono<Boolean> remove(Collection<String> key) {
        key.forEach(caches::remove);
        return clusterCache
                .remove(key)
                .then(notifyRemoveKey("__all"))
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> clear() {
        caches.clear();
        return clusterCache
                .clear()
                .then(notifyRemoveKey("__all"))
                .thenReturn(true);
    }

    void clearLocalCache(String key) {
        if ("__all".equals(key)) {
            caches.clear();
        } else if (key != null) {
            caches.remove(key);
        }
    }

    Mono<Void> notifyRemoveKey(String key) {
        return eventBus
                .publish("/_sys/cluster_cache/" + id, key)
                .then();
    }
}
