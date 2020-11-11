package org.jetlinks.supports.config;

import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.supports.cluster.EventBusLocalCache;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EventBusStorageManager implements ConfigStorageManager {

    private final ClusterManager clusterManager;

    private final EventBus eventBus;

    private final Map<String, ClusterConfigStorage> storageMap = new ConcurrentHashMap<>();

    @SuppressWarnings("all")
    public EventBusStorageManager(ClusterManager clusterManager, EventBus eventBus) {
        this.clusterManager = clusterManager;
        this.eventBus = eventBus;
        eventBus
                .subscribe(Subscription
                                   .of("event-bus-storage-listener",
                                       new String[]{"/_sys/cluster_cache/*/*/*"},
                                       Subscription.Feature.broker))
                .subscribe(payload -> {
                    Map<String, String> vars = payload.getTopicVars("/_sys/cluster_cache/{name}/{type}/{key}");

                    ClusterConfigStorage storage = storageMap.get(vars.get("name"));
                    if (storage != null) {
                        EventBusLocalCache eventBusLocalCache = ((EventBusLocalCache) storage.getCache());
                        eventBusLocalCache.clearLocalCache(vars.get("key"));
                    }
                });
    }

    @Override
    public Mono<ConfigStorage> getStorage(String id) {
        return Mono.just(storageMap.computeIfAbsent(id, __ -> new ClusterConfigStorage(new EventBusLocalCache<>(id, eventBus, clusterManager))));
    }
}
