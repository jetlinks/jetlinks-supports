package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ClusterTopic;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public class ClusterLocalCache<K, V> extends AbstractLocalCache<K, V> {

    private final ClusterTopic<K> clearTopic;

    public ClusterLocalCache(String name, ClusterManager clusterManager) {
        this(name, clusterManager, clusterManager.getCache(name), CacheBuilder.newBuilder().build());
    }

    public ClusterLocalCache(String name,
                             ClusterManager clusterManager,
                             ClusterCache<K, V> clusterCache,
                             Cache<K, Object> localCache) {
        super(clusterCache, localCache);
        this.clearTopic = clusterManager.getTopic("_local_cache_modify:".concat(name));
    }


    @Override
    protected Mono<Void> onUpdate(K key, V value) {
        return clearTopic
                .publish(Mono.just(key))
                .then();
    }

    @Override
    protected Mono<Void> onRemove(K key) {
        return clearTopic
                .publish(Mono.just(key))
                .then();
    }

    @Override
    protected Mono<Void> onRemove(Collection<? extends K> key) {
        return clearTopic
                .publish(Flux.fromIterable(key))
                .then();
    }

    @Override
    protected Mono<Void> onClear() {
        return clearTopic
                .publish(Mono.just((K) "___all"))
                .then();
    }

}
