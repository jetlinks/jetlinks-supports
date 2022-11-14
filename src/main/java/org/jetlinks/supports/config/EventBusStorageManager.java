package org.jetlinks.supports.config;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class EventBusStorageManager implements ConfigStorageManager, Disposable {

    private static final AtomicReferenceFieldUpdater<EventBusStorageManager, Disposable>
            CLUSTER_SUBSCRIBER = AtomicReferenceFieldUpdater.newUpdater(
            EventBusStorageManager.class, Disposable.class, "disposable"
    );
    final ConcurrentMap<String, LocalCacheClusterConfigStorage> cache;

    private volatile Disposable disposable;

    private final EventBus eventBus;
    private final Function<String, LocalCacheClusterConfigStorage> storageBuilder;

    @Deprecated
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus) {

        this(clusterManager, eventBus, () -> CacheBuilder.newBuilder().build());
    }

    @Deprecated
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  Supplier<Cache<String, Object>> supplier) {
        this(clusterManager, eventBus, supplier, true);
    }

    @Deprecated
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  Supplier<Cache<String, Object>> supplier,
                                  boolean cacheEmpty) {
        this(clusterManager, eventBus, -1);
    }

    @SuppressWarnings("all")
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  long ttl) {
        this.cache = new NonBlockingHashMap<>();
        this.eventBus = eventBus;
        storageBuilder = id -> {
            return new LocalCacheClusterConfigStorage(
                    id,
                    eventBus,
                    clusterManager.createCache(id),
                    ttl,
                    () -> {
                        this.cache.remove(id);
                    });
        };
    }

    private Disposable subscribeCluster() {
        return eventBus
                .subscribe(Subscription
                                   .builder()
                                   .subscriberId("event-bus-storage-listener")
                                   .topics("/_sys/cluster_cache/*")
                                   .justBroker()
                                   .build()
                )
                .subscribe(payload -> {
                    try {
                        Map<String, String> vars = payload.getTopicVars("/_sys/cluster_cache/{name}");
                        String key = payload.bodyToString();
                        LocalCacheClusterConfigStorage storage = cache.get(vars.get("name"));
                        if (storage != null) {
                            log.trace("clear local cache :{}", vars);
                            storage.clearLocalCache(key);
                        } else {
                            log.trace("ignore clear local cache :{}", vars);
                        }
                    } catch (Throwable error) {
                        log.warn("clearn local cache error", error);
                    }
                });
    }

    @Override
    public void dispose() {
        if (disposable != null) {
            disposable.dispose();
        }
    }

    @Override
    @SneakyThrows
    public Mono<ConfigStorage> getStorage(String id) {
        if (disposable == null) {
            synchronized (this){
                Disposable disp = subscribeCluster();
                if (!CLUSTER_SUBSCRIBER.compareAndSet(this, null, disp)) {
                    disp.dispose();
                }
            }
        }
        return Mono.fromSupplier(() -> cache.computeIfAbsent(id, storageBuilder));
    }
}
