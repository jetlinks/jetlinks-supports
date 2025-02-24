package org.jetlinks.supports.config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;

@Slf4j
public class EventBusStorageManager implements ConfigStorageManager, Disposable {

    static final String NOTIFY_TOPIC = "/_sys/cluster_cache";
    private final AtomicBoolean CLUSTER_SUBSCRIBER = new AtomicBoolean();
    final ConcurrentMap<String, LocalCacheClusterConfigStorage> cache;

    private final Disposable.Composite disposable = Disposables.composite();

    private final EventBus eventBus;
    private final Function<String, LocalCacheClusterConfigStorage> storageBuilder;

    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus) {

        this(clusterManager, eventBus, -1);
    }

    @SuppressWarnings("all")
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  long ttl) {
        this.cache = new ConcurrentHashMap<>();
        this.eventBus = eventBus;
        Consumer<LocalCacheClusterConfigStorage> removeHandler = store -> {
            this.cache.remove(store.id, store);
        };
        storageBuilder = id -> {
            return new LocalCacheClusterConfigStorage(
                id,
                eventBus,
                clusterManager.createCache(id),
                ttl,
                removeHandler);
        };
        if (ttl > 0) {
            Scheduler scheduler = Schedulers
                .newSingle("configs-storage-cleaner");
            disposable.add(scheduler);
            //间隔60秒 清理一次过期数据
            disposable.add(scheduler.schedulePeriodically(
                this::cleanup,
                120,
                60,
                TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("all")
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  Supplier<ConcurrentMap<String, Object>> cacheSupplier) {
        this.eventBus = eventBus;
        this.cache = (ConcurrentMap) cacheSupplier.get();
        Consumer<LocalCacheClusterConfigStorage> removeHandler = store -> {
            this.cache.remove(store.id, store);
        };
        storageBuilder = id -> new LocalCacheClusterConfigStorage(
            id,
            eventBus,
            clusterManager.createCache(id),
            -1,
            removeHandler,
            (Map) cacheSupplier.get());
    }

    private Disposable subscribeCluster() {
        return eventBus
            .subscribe(
                Subscription
                    .builder()
                    .subscriberId("event-bus-storage-listener")
                    .topics(NOTIFY_TOPIC)
                    .justBroker()
                    .build(),
                (topicPayload -> {
                    CacheNotify notify = topicPayload.decode(CacheNotify.class);
                    return Mono
                        .<Void>fromRunnable(() -> handleNotify(notify));
                })
            );
    }

    private void handleNotify(CacheNotify cacheNotify) {
        try {
            LocalCacheClusterConfigStorage storage = cache.get(cacheNotify.getName());
            if (storage != null) {
                log.trace("clear local cache :{}", cacheNotify);
                storage.clearLocalCache(cacheNotify);
            } else {
                log.trace("ignore clear local cache :{}", cacheNotify);
            }
        } catch (Throwable error) {
            log.warn("clear local cache error", error);
        }
    }

    public void refreshAll() {
        for (Map.Entry<String, LocalCacheClusterConfigStorage> entry : cache.entrySet()) {
            entry.getValue().clearLocalCache(CacheNotify.expiresAll(entry.getKey()));
        }
    }

    @Override
    public void dispose() {
        disposable.dispose();
    }

    void cleanup() {
        cache
            .forEach((key, value) -> {
                value.cleanup();
                if (value.isEmpty()) {
                    cache.remove(key, value);
                }
            });
    }

    @Override
    @SneakyThrows
    public Mono<ConfigStorage> getStorage(String id) {
        if (CLUSTER_SUBSCRIBER.compareAndSet(false, true)) {
            disposable.add(subscribeCluster());
        }
        return Mono.fromSupplier(() -> cache.computeIfAbsent(id, storageBuilder));
    }
}
