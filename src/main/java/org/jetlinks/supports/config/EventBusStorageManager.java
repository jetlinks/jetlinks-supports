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
import org.jetlinks.core.lang.SharedPathString;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.core.utils.CompositeMap;
import org.jetlinks.core.utils.TopicUtils;
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
import javax.management.openmbean.CompositeData;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * L1 configuration storage backed by the cluster cache. Changes invalidate local entries
 * through the EventBus; delivery is not a distributed commit acknowledgement. One MBean
 * exposes bounded node statistics and passive inspection for this manager's lifecycle.
 */
@Slf4j
public class EventBusStorageManager implements ConfigStorageManager, ConfigStorageCacheNotifier, Disposable {

    static final String NOTIFY_TOPIC = "/_sys/cluster_cache";
    private final AtomicBoolean CLUSTER_SUBSCRIBER = new AtomicBoolean();
    final ConcurrentMap<String, LocalCacheClusterConfigStorage> cache;
    final ConfigStorageMonitor monitor = new ConfigStorageMonitor();
    private final long ttl;
    private String namespace = "";
    private ObjectName registeredName;
    private volatile Disposable clusterSubscription;
    private Iterator<LocalCacheClusterConfigStorage> countIterator;
    private long scanStartedAt;
    private long scannedEntries;
    private long visitedStorages;
    private long nextScanAt;
    private volatile boolean countScanComplete;
    private volatile CountSnapshot counts = new CountSnapshot(-1, 0, 0, 0);

    private final Disposable.Composite disposable = Disposables.composite();

    private final Set<Consumer<CacheNotify>> cacheNotifyListeners = ConcurrentHashMap.newKeySet();

    private final EventBus eventBus;
    private final Function<String, LocalCacheClusterConfigStorage> storageBuilder;

    @Setter
    private Function<String, StorageFilter> filterFactory;

    private SharedPathString[] notifyTopics = {SharedPathString.of(NOTIFY_TOPIC)};

    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus) {

        this(clusterManager, eventBus, -1);
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace == null ? "" : namespace;
        notifyTopics = TopicUtils
            .expand(NOTIFY_TOPIC + "/" + namespace)
            .stream()
            .map(SharedPathString::of)
            .toArray(SharedPathString[]::new);
    }

    @SuppressWarnings("all")
    public EventBusStorageManager(ClusterManager clusterManager,
                                  EventBus eventBus,
                                  long ttl) {
        this.ttl = ttl;
        this.cache = new ConcurrentHashMap<>();
        this.eventBus = eventBus;
        Consumer<LocalCacheClusterConfigStorage> removeHandler = store -> {
            this.cache.remove(store.id, store);
        };
        storageBuilder = id -> {
            return new LocalCacheClusterConfigStorage(
                id,
                this,
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
        this.ttl = -1;
        this.eventBus = eventBus;
        this.cache = (ConcurrentMap) cacheSupplier.get();
        Consumer<LocalCacheClusterConfigStorage> removeHandler = store -> {
            this.cache.remove(store.id, store);
        };
        storageBuilder = id -> new LocalCacheClusterConfigStorage(
            id,
            this,
            clusterManager.createCache(id),
            -1,
            removeHandler,
            (Map) cacheSupplier.get());
    }

    private Disposable subscribeCluster() {
        SharedPathString pathString = SharedPathString.of("/_sys/storage-manager/notify");
        return eventBus
            .subscribe(
                Subscription
                    .builder()
                    .subscriberId("event-bus-storage-listener")
                    .topics(notifyTopics)
                    .justBroker()
                    .build(),
                (topicPayload -> {
                    CacheNotify notify = topicPayload.decode(CacheNotify.class);
                    return Mono
                        .<Void>fromRunnable(() -> handleNotify(notify))
                        .as(MonoTracer.create(pathString.append(notify.getName())));
                })
            );
    }

    void handleNotify(CacheNotify cacheNotify) {
        monitor.notificationsReceived.increment();
        try {
            LocalCacheClusterConfigStorage storage = cache.get(cacheNotify.getName());
            if (storage != null) {
                log.trace("clear local cache :{}", cacheNotify);
                storage.clearLocalCache(cacheNotify);
            } else {
                monitor.notificationsWithoutStorage.increment();
                log.trace("ignore clear local cache :{}", cacheNotify);
            }
        } catch (Throwable error) {
            monitor.notificationProcessingFailures.increment();
            monitor.error(error);
            log.warn("clear local cache error", error);
        }
        monitor.lastNotificationAt = System.currentTimeMillis();
        notifyListeners(cacheNotify);
    }

    Mono<Void> doNotify(CacheNotify notify) {
        return Mono.defer(() -> {
            notifyListeners(notify);
            if (notifyTopics.length == 1) {
                return eventBus
                    .publish(notifyTopics[0], notify)
                    .then();
            }

            return Flux
                .fromArray(notifyTopics)
                .flatMap(t -> eventBus.publish(t, notify), notifyTopics.length)
                .then();
        });
    }

    @Override
    public Disposable listenCacheNotify(Consumer<CacheNotify> listener) {
        cacheNotifyListeners.add(Objects.requireNonNull(listener, "listener"));
        return () -> cacheNotifyListeners.remove(listener);
    }

    private void notifyListeners(CacheNotify notify) {
        for (Consumer<CacheNotify> listener : cacheNotifyListeners) {
            try {
                listener.accept(notify);
            } catch (Throwable error) {
                monitor.notificationProcessingFailures.increment();
                monitor.error(error);
                log.warn("handle config cache notify error", error);
            }
        }
    }

    public void refreshAll() {
        monitor.refreshes.increment();
        for (Map.Entry<String, LocalCacheClusterConfigStorage> entry : cache.entrySet()) {
            CacheNotify notify = CacheNotify.expiresAll(entry.getKey());
            entry.getValue().clearLocalCache(notify);
            notifyListeners(notify);
        }
    }

    @Override
    public synchronized void dispose() {
        disposable.dispose();
        cacheNotifyListeners.clear();
        countIterator = null;
        clusterSubscription = null;
        if (registeredName != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(registeredName);
            } catch (Exception error) {
                log.warn("unregister config storage MBean error", error);
            }
            registeredName = null;
        }
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
            try {
                clusterSubscription = subscribeCluster();
                disposable.add(clusterSubscription);
            } catch (RuntimeException error) {
                CLUSTER_SUBSCRIBER.set(false);
                monitor.error(error);
                throw error;
            }
        }
        return Mono.fromSupplier(() -> cache.computeIfAbsent(id, storageBuilder));
    }

    // Count only map sizes, never cached values. A getter or another observer cannot start a scan.
    synchronized void scanCounts(long now) {
        if (disposable.isDisposed() || now < nextScanAt) {
            return;
        }
        try {
            if (countIterator == null) {
                countIterator = cache.values().iterator();
                scanStartedAt = now;
                scannedEntries = 0;
                visitedStorages = 0;
                countScanComplete = false;
            }
            if (now - scanStartedAt >= 120_000) {
                countIterator = null;
                nextScanAt = now + 30_000;
                return;
            }
            long deadline = System.nanoTime() + 5_000_000;
            int visited = 0;
            while (countIterator.hasNext() && visited < 65_536) {
                scannedEntries += countIterator.next().localEntryCount();
                visitedStorages++;
                if ((++visited & 255) == 0 && System.nanoTime() >= deadline) {
                    return;
                }
            }
            if (!countIterator.hasNext()) {
                counts = new CountSnapshot(scannedEntries, visitedStorages, scanStartedAt, now);
                countScanComplete = true;
                countIterator = null;
                nextScanAt = now + 30_000;
            }
        } catch (RuntimeException error) {
            countIterator = null;
            countScanComplete = false;
            nextScanAt = now + 30_000;
            monitor.error(error);
            log.warn("count local config storage entries error", error);
        }
    }

    private static final class CountSnapshot {
        final long entries;
        final long storages;
        final long startedAt;
        final long completedAt;

        CountSnapshot(long entries, long storages, long startedAt, long completedAt) {
            this.entries = entries;
            this.storages = storages;
            this.startedAt = startedAt;
            this.completedAt = completedAt;
        }
    }

    class ConfigStorageManagerMbeanImpl implements ConfigStorageManagerMbean {

        @Override
        public CompositeData getStatistics() {
            CountSnapshot current = counts;
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("storageCount", (long) cache.size());
            values.put("localEntryCount", current.entries);
            values.put("countVisitedStorages", current.storages);
            values.put("countScanStartedAt", current.startedAt);
            values.put("countScanCompletedAt", current.completedAt);
            values.put("countScanComplete", countScanComplete);
            values.put("idleTtlMillis", ttl);
            values.put("namespace", namespace);
            Disposable subscription = clusterSubscription;
            values.put("notificationSubscriptionActive", subscription != null && !subscription.isDisposed() && !disposable.isDisposed());
            return monitor.snapshot(values);
        }

        @Override
        public boolean isReadSamplingEnabled() {
            return monitor.reads != null;
        }

        @Override
        public void configureReadSampling(boolean enabled) {
            monitor.setReadSamplingEnabled(enabled);
        }

        @Override
        public CompositeData[] inspect(String storageId, int maxKeys) {
            LocalCacheClusterConfigStorage storage = cache.get(storageId);
            return storage == null ? new CompositeData[0] : storage.inspect(Math.max(1, Math.min(128, maxKeys)));
        }

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
                    Throwable err = e.getValue().lastError;
                    if (err != null) {
                        StringWriter writer = new StringWriter();
                        err.printStackTrace(new PrintWriter(writer));
                        info.lastError = writer.toString();
                    }
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
        private String lastError;
        private Map<String, Object> values;
    }

    /** Node-level observations; counts are weakly consistent, inspection never loads or renews L1. */
    public interface ConfigStorageManagerMbean {

        CompositeData getStatistics();

        boolean isReadSamplingEnabled();

        /** Changes only read sampling; starting/stopping a sample window creates a new epoch. */
        void configureReadSampling(boolean enabled);

        /** At most 128 local keys and their states, without values or a read from Redis. */
        CompositeData[] inspect(String storageId, int maxKeys);

        long getCacheSize();

        void cleanup();

        void clean();

        List<ConfigStorageInfo> search(String expr, int maxSize);
    }

    public interface StorageFilter {

        Set<String> getKeys();

        Mono<Boolean> test(Map<String, Object> data);

    }

    public synchronized void registerMbean() {
        if (registeredName != null || disposable.isDisposed()) {
            return;
        }
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("org.jetlinks:type=ConfigStorage,name=LocalCacheClusterConfigStorage");
            mBeanServer.registerMBean(
                new StandardMBean(new ConfigStorageManagerMbeanImpl(), ConfigStorageManagerMbean.class), objectName);
            registeredName = objectName;
            disposable.add(Schedulers.parallel().schedulePeriodically(
                () -> scanCounts(System.currentTimeMillis()), 1, 1, TimeUnit.SECONDS));
        } catch (Exception error) {
            log.warn("register config storage MBean error", error);
        }
    }
}
