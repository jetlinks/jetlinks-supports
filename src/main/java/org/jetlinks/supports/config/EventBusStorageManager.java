package org.jetlinks.supports.config;

import com.google.common.collect.Collections2;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.core.utils.CompositeMap;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class EventBusStorageManager implements ConfigStorageManager, Disposable {

    static final String NOTIFY_TOPIC = "/_sys/cluster_cache";
    private static final AtomicBoolean CLUSTER_SUBSCRIBER = new AtomicBoolean();
    final ConcurrentMap<String, LocalCacheClusterConfigStorage> cache;

    private final Disposable.Composite disposable = Disposables.composite();

    private final EventBus eventBus;
    private final Function<String, LocalCacheClusterConfigStorage> storageBuilder;

    @Setter
    private Function<String, StorageFilter> filterFactory;

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
        storageBuilder = id -> new LocalCacheClusterConfigStorage(
            id,
            eventBus,
            clusterManager.createCache(id),
            -1,
            () -> {
                this.cache.remove(id);
            },
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
                        .<Void>fromRunnable(() -> handleNotify(notify))
                        .as(MonoTracer.create("/_sys/storage-manager/notify/" + notify.getName()));
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

    class ConfigStorageManagerMbeanImpl implements ConfigStorageManagerMbean {

        @Override
        public long getCacheSize() {
            return cache.size();
        }

        @Override
        public void cleanup() {
            EventBusStorageManager.this.cleanup();
        }

        @Override
        public void clean() {
            cache.clear();
        }

        @Override
        public List<ConfigStorageInfo> search(String expr, int maxSize) {
            StorageFilter filter = filterFactory == null ? null : (StringUtils.hasText(expr) ? filterFactory.apply(expr) : null);
            maxSize = Math.max(1, maxSize);
            if (filter == null) {
                return cache
                    .entrySet()
                    .stream()
                    .limit(maxSize)
                    .map(e -> {
                        ConfigStorageInfo info = new ConfigStorageInfo();
                        info.setId(e.getKey());
                        info.setValues(e.getValue().getAll());
                        return info;
                    })
                    .collect(Collectors.toList());
            }
            return Flux
                .fromIterable(cache.entrySet())
                .flatMap(e -> e
                             .getValue()
                             .getConfigs(
                                 Collections2.filter(filter.getKeys(), id -> !id.equals("id"))
                             )
                             .thenReturn(e),
                         32)
                .publishOn(Schedulers.boundedElastic())
                .filterWhen(e -> filter
                                .test(
                                    new CompositeMap<>(
                                        Collections.singletonMap("id", e.getKey()),
                                        e.getValue().getAll()
                                    )),
                            32)
                .take(maxSize)
                .map(e -> {
                    ConfigStorageInfo info = new ConfigStorageInfo();
                    info.setId(e.getKey());
                    info.setValues(e.getValue().getAll());
                    return info;
                })
                .collectList()
                .block(Duration.ofSeconds(30));
        }
    }

    @Getter
    @Setter
    public static class ConfigStorageInfo {
        private String id;

        private Map<String, Object> values;
    }

    public interface ConfigStorageManagerMbean {

        long getCacheSize();

        void cleanup();

        void clean();

        List<ConfigStorageInfo> search(String expr, int maxSize);
    }

    public interface StorageFilter {

        Set<String> getKeys();

        Mono<Boolean> test(Map<String, Object> data);

    }

    public void registerMbean() {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("org.jetlinks:type=ConfigStorage,name=LocalCacheClusterConfigStorage");
            mBeanServer.registerMBean(
                new StandardMBean(new ConfigStorageManagerMbeanImpl(), ConfigStorageManagerMbean.class), objectName);
        } catch (Throwable ignore) {
        }
    }
}
