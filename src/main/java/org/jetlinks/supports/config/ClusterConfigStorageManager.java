package org.jetlinks.supports.config;

import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.supports.cluster.ClusterLocalCache;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterConfigStorageManager implements ConfigStorageManager {

    private final ClusterManager clusterManager;

    private final Map<String, ClusterConfigStorage> storageMap = new ConcurrentHashMap<>();

    @SuppressWarnings("all")
    public ClusterConfigStorageManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        clusterManager.getTopic("_local_cache_modify:*")
                .subscribePattern()
                .subscribe(msg -> {
                    String[] type = msg.getTopic().split("[:]", 2);
                    if (type.length <= 0) {
                        return;
                    }
                    Optional.ofNullable(storageMap.get(type[1]))
                            .ifPresent(store -> ((ClusterLocalCache) store.getCache()).clearLocalCache(msg.getMessage()));

                });
    }

    @Override
    public Mono<ConfigStorage> getStorage(String id) {
        return Mono.just(storageMap.computeIfAbsent(id, __ -> new ClusterConfigStorage(new ClusterLocalCache<>(id, clusterManager))));
    }
}
