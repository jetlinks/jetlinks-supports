package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.lang.SharedPathString;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.After;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Deterministic business races and the public, passive JMX observation boundary. */
public class ConfigStorageMonitorTest {
    private final List<EventBusStorageManager> managers = new ArrayList<>();

    @After
    public void cleanup() {
        managers.forEach(EventBusStorageManager::dispose);
    }

    @Test
    public void failedAndUnsubscribedWritesKeepCommittedValue() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.just("old"));
        AtomicInteger writes = new AtomicInteger();
        when(f.backend.put("name", "new")).thenReturn(Mono.defer(() -> {
            writes.incrementAndGet();
            return Mono.error(new IllegalStateException("write failed"));
        }));
        when(f.backend.putAll(Collections.singletonMap("name", "new")))
            .thenReturn(Mono.error(new IllegalStateException("write failed")));
        assertEquals("old", f.storage.getConfig("name").block().asString());

        Mono<Boolean> write = f.storage.setConfig("name", "new");
        Mono<Boolean> batchWrite = f.storage.setConfigs(Collections.singletonMap("name", "new"));
        assertEquals(0, writes.get());
        assertEquals("old", f.storage.getConfig("name").block().asString());
        StepVerifier.create(write).expectError(IllegalStateException.class).verify();
        StepVerifier.create(batchWrite).expectError(IllegalStateException.class).verify();
        assertEquals("old", f.storage.getConfig("name").block().asString());
        assertEquals(1, writes.get());
        assertEquals(2, f.manager.monitor.writeFailures.sum());
        assertEquals(0, f.manager.monitor.notificationsSent.sum());
    }

    @Test
    public void aCommittedWriteCompletesOldWaitersAndInvalidates() {
        Fixture f = fixture(-1, new InternalEventBus());
        Sinks.One<Object> read = Sinks.one();
        Sinks.One<Boolean> write = Sinks.one();
        when(f.backend.get("name")).thenReturn(read.asMono(), Mono.just("new"));
        when(f.backend.put("name", "new")).thenReturn(write.asMono());
        CompletableFuture<Value> oldReader = f.storage.getConfig("name").toFuture();
        CompletableFuture<Boolean> writing = f.storage.setConfig("name", "new").toFuture();
        assertFalse(oldReader.isDone());
        write.emitValue(true, Sinks.EmitFailureHandler.FAIL_FAST);
        assertTrue(writing.join());
        assertEquals("new", oldReader.join().asString());
        assertEquals("new", f.storage.getConfig("name").block().asString());
        assertEquals(0, f.manager.monitor.loadsInFlight.get());
        assertEquals(1, f.manager.monitor.loadCancellations.sum());
    }

    @Test
    public void remoteInvalidationDetachesAnInFlightSingleKeyRead() {
        Fixture f = fixture(-1, new InternalEventBus());
        Sinks.One<Object> old = Sinks.one();
        when(f.backend.get("name")).thenReturn(old.asMono(), Mono.just("new"));
        Mono<Value> oldRead = f.storage.getConfig("name");
        f.manager.handleNotify(CacheNotify.expires("device:test", Collections.singleton("name")));
        assertEquals("new", f.storage.getConfig("name").block().asString());
        old.emitValue("old", Sinks.EmitFailureHandler.FAIL_FAST);
        assertEquals("old", oldRead.block().asString());
        assertEquals("new", f.storage.getConfig("name").block().asString());
        verify(f.backend, times(2)).get("name");
        assertEquals(1, f.manager.monitor.invalidatedLoads.sum());
        assertEquals(1, f.manager.monitor.notificationsReceived.sum());
        assertEquals(0, f.manager.monitor.loadsInFlight.get());
    }

    @Test
    public void lateBatchPositiveAndNegativeResultsCannotRepopulateNewEntries() {
        for (boolean hasOldValue : new boolean[]{true, false}) {
            Fixture f = fixture(-1, new InternalEventBus());
            Sinks.One<Map.Entry<String, Object>> old = Sinks.one();
            when(f.backend.get(anyCollection())).thenReturn(old.asMono().flux(),
                Flux.just(new AbstractMap.SimpleImmutableEntry<>("name", "new")));
            CompletableFuture<Values> oldRead = f.storage.getConfigs(Collections.singleton("name")).toFuture();
            f.manager.handleNotify(CacheNotify.expires("device:test", Collections.singleton("name")));
            assertEquals("new", f.storage.getConfigs(Collections.singleton("name")).block().getString("name", ""));
            if (hasOldValue) {
                old.emitValue(new AbstractMap.SimpleImmutableEntry<>("name", "old"), Sinks.EmitFailureHandler.FAIL_FAST);
            } else {
                old.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
            }
            assertEquals(hasOldValue ? "old" : "", oldRead.join().getString("name", ""));
            assertEquals("new", f.storage.getConfig("name").block().asString());
            verify(f.backend, times(2)).get(anyCollection());
        }
    }

    @Test
    public void retryAndResubscriptionHaveIndependentBatchState() {
        Fixture f = fixture(-1, new InternalEventBus());
        AtomicInteger attempts = new AtomicInteger();
        when(f.backend.get(anyCollection())).thenReturn(Flux.defer(() -> attempts.incrementAndGet() == 1
            ? Flux.error(new IllegalStateException("temporarily unavailable"))
            : Flux.just(new AbstractMap.SimpleImmutableEntry<>("name", "value"))));
        Mono<Values> read = f.storage.getConfigs(Collections.singleton("name"));
        assertEquals("value", read.retry(1).block().getString("name", ""));
        assertEquals("value", read.block().getString("name", ""));
        assertEquals(3, f.manager.monitor.loadRequests.sum());
        assertEquals(2, f.manager.monitor.loadSuccesses.sum());
        assertEquals(1, f.manager.monitor.loadFailures.sum());
        assertEquals(0, f.manager.monitor.loadsInFlight.get());
    }

    @Test
    public void sampledDecisionsCountPositiveNegativeAndFastPathFallbackOnce() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get(anyCollection())).thenReturn(Flux.just(
            new AbstractMap.SimpleImmutableEntry<>("name", "value")));
        f.storage.getConfigs(Arrays.asList("name", "absent", "other")).block();
        f.manager.monitor.reads = new ConfigStorageMonitor.ReadCounters(1);
        f.storage.getConfigs(Arrays.asList("name", "absent")).block();
        f.storage.getConfigs(Arrays.asList("name", "absent", "other")).block();
        f.storage.getConfigs(Arrays.asList("name", "name", "name")).block();
        f.storage.clearLocalCache(CacheNotify.expires("device:test", Collections.singleton("name")));
        f.storage.getConfigs(Arrays.asList("name", "absent")).block();
        ConfigStorageMonitor.ReadCounters r = f.manager.monitor.reads;
        assertEquals(4, r.calls.sum());
        assertEquals(5, r.valueHits.sum());
        assertEquals(4, r.negativeHits.sum());
        assertEquals(1, r.misses.sum());
    }

    @Test
    public void sharingALoadIsAMissAndDoesNotIssueAnotherPhysicalRead() {
        Fixture f = fixture(-1, new InternalEventBus());
        f.manager.monitor.reads = new ConfigStorageMonitor.ReadCounters(1);
        Sinks.One<Object> source = Sinks.one();
        when(f.backend.get("name")).thenReturn(source.asMono());
        Mono<Value> first = f.storage.getConfig("name");
        Mono<Value> second = f.storage.getConfig("name");
        assertEquals(2, f.manager.monitor.reads.misses.sum());
        assertEquals(0, f.manager.monitor.reads.valueHits.sum());
        assertEquals(1, f.manager.monitor.loadRequests.sum());
        source.emitValue("value", Sinks.EmitFailureHandler.FAIL_FAST);
        assertEquals("value", first.block().asString());
        assertEquals("value", second.block().asString());
    }

    @Test
    public void cancelledBatchReleasesItsInFlightCount() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get(anyCollection())).thenReturn(Flux.never());
        Disposable reader = f.storage.getConfigs(Collections.singleton("name")).subscribe();
        assertEquals(1, f.manager.monitor.loadsInFlight.get());
        reader.dispose();
        assertEquals(0, f.manager.monitor.loadsInFlight.get());
        assertEquals(1, f.manager.monitor.loadCancellations.sum());
    }

    @Test
    public void staleFallbackIsVisibleInsteadOfHidingAReadFailure() {
        Fixture f = fixture(60_000, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.just("old"), Mono.error(new IllegalStateException("down")));
        f.storage.getConfig("name").block();
        f.entries.get("name").t = System.currentTimeMillis() - 60_001;
        assertEquals("old", f.storage.getConfig("name").block().asString());
        assertEquals(1, f.manager.monitor.staleFallbacks.sum());
        assertEquals(1, f.manager.monitor.loadFailures.sum());
        assertEquals(IllegalStateException.class.getName(), f.manager.monitor.lastErrorType);
        assertEquals(0, f.manager.monitor.loadsInFlight.get());
    }

    @Test
    public void failedNotificationDoesNotPretendTheWriteWasRolledBack() {
        EventBus bus = mock(EventBus.class);
        when(bus.publish(any(SharedPathString.class), any(CacheNotify.class)))
            .thenReturn(Mono.error(new IllegalStateException("publish failed")));
        Fixture f = fixture(-1, bus);
        AtomicReference<String> committed = new AtomicReference<>("old");
        when(f.backend.get("name")).thenAnswer(ignored -> Mono.just(committed.get()));
        when(f.backend.put("name", "new")).thenReturn(Mono.fromSupplier(() -> {
            committed.set("new");
            return true;
        }));
        f.storage.getConfig("name").block();
        StepVerifier.create(f.storage.setConfig("name", "new")).expectError(IllegalStateException.class).verify();
        assertEquals("new", committed.get());
        assertEquals("new", f.storage.getConfig("name").block().asString());
        assertEquals(1, f.manager.monitor.notificationFailures.sum());
        assertEquals(1, f.manager.monitor.writeSuccesses.sum());
        assertEquals(0, f.manager.monitor.writeFailures.sum());
        assertEquals(0, f.manager.monitor.notificationsSent.sum());
    }

    @Test
    public void passiveStatisticsAndInspectionDoNotRenewOrLoadCache() {
        Fixture f = fixture(60_000, new InternalEventBus());
        when(f.backend.get(anyCollection())).thenReturn(Flux.just(new AbstractMap.SimpleImmutableEntry<>("name", "value")));
        f.storage.getConfigs(Arrays.asList("name", "missing")).block();
        long oldTime = System.currentTimeMillis() - 61_000;
        f.entries.values().forEach(entry -> entry.t = oldTime);
        EventBusStorageManager.ConfigStorageManagerMbean bean = bean(f);
        long reads = f.manager.monitor.loadRequests.sum();
        f.manager.scanCounts(System.currentTimeMillis());
        assertEquals(2L, bean.getStatistics().get("localEntryCount"));
        assertEquals(1L, bean.getStatistics().get("storageCount"));
        CompositeData[] inspection = bean.inspect("device:test", 128);
        assertEquals(2, inspection.length);
        for (CompositeData entry : inspection) {
            assertEquals("expired", entry.get("state"));
            assertFalse(entry.containsKey("value"));
        }
        assertEquals(0, bean.inspect("not-held", 128).length);
        assertEquals(1, f.manager.cache.size());
        f.entries.values().forEach(entry -> assertEquals(oldTime, entry.t));
        assertEquals(reads, f.manager.monitor.loadRequests.sum());
        verify(f.backend, times(1)).get(anyCollection());
    }

    @Test
    public void countScanIsBoundedAndDoesNotPublishAPartialTotal() {
        Fixture f = fixture(-1, new InternalEventBus());
        for (int i = 0; i < 70_000; i++) {
            String id = "device:" + i;
            f.manager.cache.put(id, new LocalCacheClusterConfigStorage(id, f.manager, f.backend, -1, null));
        }
        long now = System.currentTimeMillis();
        f.manager.scanCounts(now);
        assertEquals(-1L, bean(f).getStatistics().get("localEntryCount"));
        assertEquals(false, bean(f).getStatistics().get("countScanComplete"));
        for (int i = 0; i < 200 && !(boolean) bean(f).getStatistics().get("countScanComplete"); i++) {
            f.manager.scanCounts(now + i + 1);
        }
        assertEquals(true, bean(f).getStatistics().get("countScanComplete"));
        assertEquals(0L, bean(f).getStatistics().get("localEntryCount"));
        assertEquals(70_001L, bean(f).getStatistics().get("countVisitedStorages"));
    }

    @Test
    public void cancelledWritesDoNotExposeAnUncommittedValue() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.just("old"));
        when(f.backend.put("name", "new")).thenReturn(Mono.never());
        when(f.backend.putAll(Collections.singletonMap("name", "new"))).thenReturn(Mono.never());
        assertEquals("old", f.storage.getConfig("name").block().asString());
        Disposable single = f.storage.setConfig("name", "new").subscribe();
        Disposable batch = f.storage.setConfigs(Collections.singletonMap("name", "new")).subscribe();
        single.dispose();
        batch.dispose();
        assertEquals("old", f.storage.getConfig("name").block().asString());
        assertEquals(0, f.manager.monitor.writeSuccesses.sum());
        assertEquals(0, f.manager.monitor.notificationsSent.sum());
    }

    @Test
    public void wholeStorageInvalidationDetachesSingleAndBatchLoads() {
        for (boolean batch : new boolean[]{false, true}) {
            Fixture f = fixture(-1, new InternalEventBus());
            Sinks.One<Object> old = Sinks.one();
            when(f.backend.get("name")).thenReturn(batch ? Mono.just("new") : old.asMono(), Mono.just("new"));
            when(f.backend.get(anyCollection())).thenReturn(
                old.asMono().<Map.Entry<String, Object>>map(value -> new AbstractMap.SimpleImmutableEntry<>("name", value)).flux(),
                Flux.just(new AbstractMap.SimpleImmutableEntry<>("name", "new")));
            CompletableFuture<String> oldReader = (batch
                ? f.storage.getConfigs(Collections.singleton("name")).map(values -> values.getString("name", ""))
                : f.storage.getConfig("name").map(Value::asString)).toFuture();
            f.manager.handleNotify(CacheNotify.clear("device:test"));
            assertEquals("new", f.storage.getConfig("name").block().asString());
            old.emitValue("old", Sinks.EmitFailureHandler.FAIL_FAST);
            assertEquals("old", oldReader.join());
            assertEquals("new", f.storage.getConfig("name").block().asString());
        }
    }

    @Test
    public void inspectionLimitsRowsAndKeyLengthWithoutReadingValues() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get(anyString())).thenReturn(Mono.just("private-value"));
        char[] chars = new char[600];
        Arrays.fill(chars, 'k');
        String prefix = new String(chars);
        for (int i = 0; i < 140; i++) {
            f.storage.getConfig(prefix + i).block();
        }
        CompositeData[] entries = bean(f).inspect("device:test", Integer.MAX_VALUE);
        assertEquals(128, entries.length);
        for (CompositeData entry : entries) {
            assertEquals(512, ((String) entry.get("key")).length());
            assertEquals(true, entry.get("keyTruncated"));
            assertFalse(entry.containsKey("value"));
        }
        assertEquals(1, bean(f).inspect("device:test", 0).length);
        verify(f.backend, times(140)).get(anyString());
    }

    @Test
    public void timedOutScanKeepsLastCompleteCountAndDisposalStopsScanning() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.just("value"));
        f.storage.getConfig("name").block();
        long now = System.currentTimeMillis();
        f.manager.scanCounts(now);
        assertEquals(1L, bean(f).getStatistics().get("localEntryCount"));
        for (int i = 0; i < 70_000; i++) {
            String id = "device:" + i;
            f.manager.cache.put(id, new LocalCacheClusterConfigStorage(id, f.manager, f.backend, -1, null));
        }
        f.manager.scanCounts(now + 30_000);
        assertEquals(false, bean(f).getStatistics().get("countScanComplete"));
        f.manager.scanCounts(now + 150_000);
        CompositeData timedOut = bean(f).getStatistics();
        assertEquals(1L, timedOut.get("localEntryCount"));
        assertEquals(now, timedOut.get("countScanCompletedAt"));
        f.manager.dispose();
        f.manager.scanCounts(now + 180_000);
        assertEquals(now, bean(f).getStatistics().get("countScanCompletedAt"));
    }

    @Test
    public void standardMBeanSupportsOpenDataInspectionAndOwnedLifecycle() throws Exception {
        Fixture first = fixture(-1, new InternalEventBus());
        Fixture second = fixture(-1, new InternalEventBus());
        ObjectName name = new ObjectName("org.jetlinks:type=ConfigStorage,name=LocalCacheClusterConfigStorage");
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        first.manager.registerMbean();
        CompositeData statistics = (CompositeData) server.getAttribute(name, "Statistics");
        assertEquals(1L, statistics.get("schemaVersion"));
        assertEquals(0L, statistics.get("readSampleInterval"));
        assertEquals(false, server.getAttribute(name, "ReadSamplingEnabled"));
        server.invoke(name, "configureReadSampling", new Object[]{true}, new String[]{"boolean"});
        CompositeData enabled = (CompositeData) server.getAttribute(name, "Statistics");
        assertEquals(1024L, enabled.get("readSampleInterval"));
        assertNotEquals(statistics.get("statisticsEpoch"), enabled.get("statisticsEpoch"));
        String epoch = (String) enabled.get("statisticsEpoch");
        server.invoke(name, "configureReadSampling", new Object[]{false}, new String[]{"boolean"});
        CompositeData disabled = (CompositeData) server.getAttribute(name, "Statistics");
        assertEquals(0L, disabled.get("readSampleInterval"));
        assertNotEquals(epoch, disabled.get("statisticsEpoch"));
        assertEquals(0, ((CompositeData[]) server.invoke(name, "inspect", new Object[]{"absent", 5},
            new String[]{String.class.getName(), "int"})).length);
        second.manager.registerMbean();
        second.manager.dispose();
        assertTrue(server.isRegistered(name));
        first.manager.dispose();
        assertFalse(server.isRegistered(name));
        first.manager.registerMbean();
        assertFalse(server.isRegistered(name));
    }

    @SuppressWarnings("unchecked")
    private Fixture fixture(long ttl, EventBus bus) {
        Fixture f = new Fixture();
        f.backend = mock(ClusterCache.class);
        f.manager = new EventBusStorageManager(mock(ClusterManager.class), bus, ttl);
        managers.add(f.manager);
        f.entries = new ConcurrentHashMap<>();
        f.storage = new LocalCacheClusterConfigStorage("device:test", f.manager, f.backend, ttl, null, f.entries);
        f.manager.cache.put("device:test", f.storage);
        return f;
    }

    private EventBusStorageManager.ConfigStorageManagerMbean bean(Fixture f) {
        return f.manager.new ConfigStorageManagerMbeanImpl();
    }

    private static final class Fixture {
        ClusterCache<String, Object> backend;
        EventBusStorageManager manager;
        LocalCacheClusterConfigStorage storage;
        Map<String, LocalCacheClusterConfigStorage.Cache> entries;
    }
}
