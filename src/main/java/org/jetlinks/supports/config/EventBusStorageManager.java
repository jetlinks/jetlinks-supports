package org.jetlinks.supports.config;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.supports.cluster.EventBusLocalCache;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Supplier;

public class EventBusStorageManager implements ConfigStorageManager {

    private final ClusterManager clusterManager;

    private final EventBus eventBus;

    private final Cache<String, ClusterConfigStorage> cache;

    private final Supplier<Cache<String, Object>> cacheSupplier;

    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus) {

        this(clusterManager, eventBus, () -> CacheBuilder.newBuilder().build());
    }

    @SuppressWarnings("all")
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  Supplier<Cache<String, Object>> supplier) {
        this.clusterManager = clusterManager;
        this.eventBus = eventBus;
        this.cache = (Cache) supplier.get();
        this.cacheSupplier = supplier;
        eventBus
                .subscribe(Subscription
                                   .of("event-bus-storage-listener",
                                       new String[]{"/_sys/cluster_cache/*/*/*"},
                                       Subscription.Feature.broker))
                .subscribe(payload -> {
                    payload.release();
                    Map<String, String> vars = payload.getTopicVars("/_sys/cluster_cache/{name}/{type}/{key}");

                    ClusterConfigStorage storage = cache.getIfPresent(vars.get("name"));
                    if (storage != null) {
                        EventBusLocalCache eventBusLocalCache = ((EventBusLocalCache) storage.getCache());
                        eventBusLocalCache.clearLocalCache(vars.get("key"));
                    }
                });
    }

    @Override
    public Mono<ConfigStorage> getStorage(String id) {
        return Mono
                .fromCallable(() -> cache.get(id, () -> new ClusterConfigStorage(new EventBusLocalCache<>(
                        id,
                        eventBus,
                        clusterManager,
                        cacheSupplier.get()))));
    }
}
