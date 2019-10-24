package org.jetlinks.supports.config;

import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.supports.cluster.ClusterLocalCache;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterConfigStorageManager implements ConfigStorageManager {

    private ClusterManager clusterManager;

    public ClusterConfigStorageManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    private Map<String, ConfigStorage> storageMap = new ConcurrentHashMap<>();

    @Override
    public Mono<ConfigStorage> getStorage(String id) {
        return Mono.just(storageMap.computeIfAbsent(id, __ -> new ClusterConfigStorage(new ClusterLocalCache<>(id, clusterManager))));
    }
}
