package org.jetlinks.supports.config;

import lombok.SneakyThrows;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class SimpleConfigStorageManager implements ConfigStorageManager {

    private final Function<String, SimpleClusterConfigStorage> storageBuilder;
    private final ConcurrentMap<String, SimpleClusterConfigStorage> cache = new ConcurrentHashMap<>();

    @SuppressWarnings("all")
    public SimpleConfigStorageManager(ClusterManager clusterManager) {
        storageBuilder = id -> {
            return new SimpleClusterConfigStorage(clusterManager.getCache(id));
        };
    }

    @Override
    @SneakyThrows
    public Mono<ConfigStorage> getStorage(String id) {
        return Mono.just(cache.computeIfAbsent(id, storageBuilder));
    }
}
