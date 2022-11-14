package org.jetlinks.supports.config;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.event.EventBus;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;

@AllArgsConstructor
public class LocalCacheClusterConfigStorage implements ConfigStorage {
    private final Map<String, Cache> caches = new NonBlockingHashMap<>();
    private final String id;
    private final EventBus eventBus;

    private final ClusterCache<String, Object> clusterCache;

    private long expires;
    private final Runnable doOnClear;

    public static final Value NULL = Value.simple(null);

    class Cache {
        final String key;
        long t;
        volatile Mono<Value> ref;
        volatile Value cached;

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
            this.ref = Mono.justOrEmpty(value);
            this.cached = value == null ? NULL : value;
        }

        void reload() {
            cached = null;
            ref = clusterCache
                    .get(key)
                    .map(Value::simple)
                    .doOnNext(this::setValue)
                    .switchIfEmpty(Mono.fromRunnable(() -> this.setValue(null)))
                    .cache(v -> Duration.ofMillis(Long.MAX_VALUE),
                           error -> Duration.ofSeconds(1),
                           () -> Duration.ofSeconds(2));
        }

        void clear() {
            cached = null;
            ref = null;
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
        Map<String, Object> caches = Maps.newHashMapWithExpectedSize(keys.size());
        int hits = 0;
        //获取一级缓存
        for (String key : keys) {
            Cache local = getOrCreateCache(key);
            Value cached = local.getCached();
            if (cached != null) {
                //命中一级缓存
                hits++;
                Object obj = cached.get();
                //可能缓存的就是null(配置不存在的情况)
                if (null != obj) {
                    caches.put(key, obj);
                }
            }
        }
        //全部来自一级缓存则直接返回
        if (hits == keys.size()) {
            return Mono.just(Values.of(caches));
        }
        //需要从二级缓存中加载的配置
        Set<String> needLoadKeys = new HashSet<>(keys);
        needLoadKeys.removeAll(caches.keySet());

        return clusterCache
                .get(needLoadKeys)
                .reduce(caches, (map, entry) -> {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    //加载到一级缓存中
                    getOrCreateCache(key).setValue(value);
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
                .thenReturn(true)
                .doAfterTerminate(() -> values.forEach((key, value) -> getOrCreateCache(key).clear()));
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
                .doAfterTerminate(() -> {
                    Cache cache = caches.get(key);
                    if (cache != null) {
                        cache.clear();
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
        caches.remove(key);
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
        if (doOnClear != null) {
            doOnClear.run();
        }
        return clusterCache
                .clear()
                .then(notifyRemoveKey("__clear"))
                .thenReturn(true);
    }

    void clearLocalCache(String key) {
        if ("__all".equals(key)) {
            caches.clear();
        } else if ("__clear".equals(key)) {
            caches.clear();
            if (doOnClear != null) {
                doOnClear.run();
            }
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
