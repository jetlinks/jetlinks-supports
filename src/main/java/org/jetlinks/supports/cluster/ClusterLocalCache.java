package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ClusterTopic;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

public class ClusterLocalCache<K, V> implements ClusterCache<K, V> {

    private Cache<K, V> cache;

    private ClusterCache<K, V> clusterCache;

    private ClusterTopic<K> clearTopic;

    public ClusterLocalCache(String name, ClusterManager clusterManager) {
        this(name, clusterManager, clusterManager.getCache(name), CacheBuilder.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(30))
                .expireAfterWrite(Duration.ofMinutes(30))
                .softValues()
                .build());
    }

    public ClusterLocalCache(String name, ClusterManager clusterManager, ClusterCache<K, V> clusterCache, Cache<K, V> localCache) {
        this.clusterCache = clusterCache;
        this.cache = localCache;
        this.clearTopic = clusterManager.getTopic("_local_cache_modify:".concat(name));

        clearTopic.subscribe()
                .subscribe(key -> cache.invalidate(key));
    }

    @Override
    public Mono<V> get(K key) {
        return Mono.justOrEmpty(cache.getIfPresent(key))
                .switchIfEmpty(clusterCache.get(key).doOnNext(v -> cache.put(key, v)));
    }

    @Override
    public Flux<V> get(Collection<K> key) {
        return Mono.justOrEmpty(cache.getAllPresent(key))
                .map(ImmutableMap::values)
                .flatMapMany(Flux::fromIterable)
                .switchIfEmpty(Flux.fromIterable(key)
                        .flatMap(k -> Mono.just(k).zipWith(clusterCache.get(k)))
                        .doOnNext(tuple -> cache.put(tuple.getT1(), tuple.getT2()))
                        .map(Tuple2::getT2));
    }

    @Override
    public Mono<Boolean> put(K key, V value) {
        return Mono.defer(() -> {
            cache.invalidate(key);
            return clusterCache.put(key, value)
                    .flatMap(r -> clearTopic.publish(Mono.just(key)))
                    .thenReturn(true);
        });
    }

    @Override
    public Mono<Boolean> putIfAbsent(K key, V value) {
        return Mono.defer(() -> {
            cache.invalidate(key);
            return clusterCache.putIfAbsent(key, value)
                    .flatMap(r -> clearTopic.publish(Mono.just(key)).thenReturn(r));
        });
    }

    @Override
    public Mono<Boolean> remove(K key) {
        return Mono.defer(() -> {
            cache.invalidate(key);
            return clusterCache.remove(key)
                    .flatMap(r -> clearTopic.publish(Mono.just(key)))
                    .thenReturn(true);
        });
    }

    @Override
    public Mono<Boolean> remove(Collection<K> key) {
        return Mono.defer(() -> {
            cache.invalidateAll(key);
            return clusterCache.remove(key)
                    .flatMap(r -> clearTopic.publish(Flux.fromIterable(key)))
                    .thenReturn(true);
        });
    }

    @Override
    public Mono<Boolean> containsKey(K key) {
        return Mono.defer(() -> {
            if (clusterCache.containsKey(key) != null) {
                return Mono.just(true);
            }
            return clusterCache.containsKey(key);
        });
    }

    @Override
    public Flux<K> keys() {

        return clusterCache.keys();
    }

    @Override
    public Flux<V> values() {
        return clusterCache.values();
    }

    @Override
    public Mono<Boolean> putAll(Map<? extends K, ? extends V> multi) {
        if(CollectionUtils.isEmpty(multi)){
            return Mono.just(true);
        }
        return Mono.defer(() -> {
            cache.putAll(multi);
            return clusterCache.putAll(multi)
                    .flatMap(r -> clearTopic.publish(Flux.fromIterable(multi.keySet())))
                    .thenReturn(true);
        });
    }

    @Override
    public Mono<Integer> size() {
        return clusterCache.size();
    }

    @Override
    public Flux<Map.Entry<K, V>> entries() {
        return clusterCache.entries();
    }

    @Override
    public Mono<Void> clear() {
        return Mono.defer(() -> {
            cache.invalidateAll();
            return clusterCache.clear();
        });
    }
}
