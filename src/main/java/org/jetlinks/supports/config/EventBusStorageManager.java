package org.jetlinks.supports.config;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.util.ReferenceCountUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.supports.cluster.EventBusLocalCache;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class EventBusStorageManager implements ConfigStorageManager {

    private final ConcurrentMap<String, ClusterConfigStorage> cache;

    private final Supplier<Cache<String, Object>> cacheSupplier;

    private final Function<String, ClusterConfigStorage> storageBuilder;

    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus) {

        this(clusterManager, eventBus, () -> CacheBuilder.newBuilder().build());
    }

    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  Supplier<Cache<String, Object>> supplier) {
        this(clusterManager, eventBus, supplier, true);
    }

    @SuppressWarnings("all")
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  Supplier<Cache<String, Object>> supplier,
                                  boolean cacheEmpty) {
        this.cache = (ConcurrentMap) supplier.get().asMap();
        this.cacheSupplier = supplier;
        storageBuilder = id -> {
            return new ClusterConfigStorage(new EventBusLocalCache<>(id,
                                                                     eventBus,
                                                                     clusterManager.getCache(id),
                                                                     cacheSupplier.get(),
                                                                     cacheEmpty));
        };
        eventBus
                .subscribe(Subscription
                                   .builder()
                                   .subscriberId("event-bus-storage-listener")
                                   .topics("/_sys/cluster_cache/*/*/*")
                                   .justBroker()
                                   .build()
                )
                .subscribe(payload -> {
                    try {
                        Map<String, String> vars = payload.getTopicVars("/_sys/cluster_cache/{name}/{type}/{key}");

                        ClusterConfigStorage storage = cache.get(vars.get("name"));
                        if (storage != null) {
                            log.trace("clear local cache :{}", vars);
                            EventBusLocalCache eventBusLocalCache = ((EventBusLocalCache) storage.getCache());
                            eventBusLocalCache.clearLocalCache(vars.get("key"));
                        } else {
                            log.trace("ignore clear local cache :{}", vars);
                        }
                    } catch (Throwable error) {
                        log.warn("clearn local cache error", error);
                    } finally {
                        ReferenceCountUtil.safeRelease(payload);
                    }
                });
    }

    @Override
    @SneakyThrows
    public Mono<ConfigStorage> getStorage(String id) {
        return Mono.just(cache.computeIfAbsent(id, storageBuilder));
    }
}
