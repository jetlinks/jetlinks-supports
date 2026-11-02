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
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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
    }

    @Test
    public void remoteInvalidationDetachesAnInFlightSingleKeyRead() {
        Fixture f = fixture(-1, new InternalEventBus());
        Sinks.One<Object> old = Sinks.one();
        when(f.backend.get("name")).thenReturn(old.asMono(), Mono.just("new"));
        CompletableFuture<Value> oldRead = f.storage.getConfig("name").toFuture();
        f.manager.handleNotify(CacheNotify.expires("device:test", Collections.singleton("name")));
        assertEquals("new", f.storage.getConfig("name").block().asString());
        old.emitValue("old", Sinks.EmitFailureHandler.FAIL_FAST);
        assertEquals("old", oldRead.join().asString());
        assertEquals("new", f.storage.getConfig("name").block().asString());
        verify(f.backend, times(2)).get("name");
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
        assertEquals(1, f.manager.monitor.loadErrors.sum());
    }

    @Test
    public void statisticsExposeCapacityAndFaultsWithoutLoadActivityMetrics() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get(anyCollection())).thenReturn(Flux.just(
            new AbstractMap.SimpleImmutableEntry<>("name", "value")));
        f.storage.getConfigs(Arrays.asList("name", "missing")).block();
        f.storage.getConfig("name").block();
        f.storage.getConfigs(Arrays.asList("name", "missing", "name")).block();
        CompositeData statistics = bean(f).getStatistics();
        assertEquals(new HashSet<>(Arrays.asList(
            "schemaVersion", "instanceId", "capturedAt", "storageCount", "localEntryCount",
            "countCapturedAt", "idleTtlMillis", "namespace", "notificationSubscriptionActive",
            "loadErrors", "staleFallbacks", "invalidationErrors", "listenerErrors",
            "lastErrorStage", "lastErrorType", "lastErrorAt")), statistics.getCompositeType().keySet());
        assertEquals(0L, statistics.get("loadErrors"));
        assertEquals(0L, statistics.get("staleFallbacks"));
        assertEquals(0L, statistics.get("invalidationErrors"));
        assertEquals(0L, statistics.get("listenerErrors"));
        assertEquals("", statistics.get("lastErrorStage"));
        assertEquals("", statistics.get("lastErrorType"));
        assertEquals(0L, statistics.get("lastErrorAt"));
    }

    @Test
    public void repeatedBatchFiltersDuplicateAndNegativeRecordsWithoutMutatingTargets() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get(anyCollection())).thenReturn(Flux.just(
            new AbstractMap.SimpleImmutableEntry<>("name", "first"),
            new AbstractMap.SimpleImmutableEntry<>("name", "duplicate"),
            new AbstractMap.SimpleImmutableEntry<>("negative", null),
            new AbstractMap.SimpleImmutableEntry<>("unexpected", "ignored")));
        Mono<Values> read = f.storage.getConfigs(Arrays.asList("name", "negative", "missing"));
        for (int i = 0; i < 2; i++) {
            if (i != 0) f.storage.clearLocalCache(CacheNotify.expiresAll("device:test"));
            assertEquals(Collections.singletonMap("name", "first"), read.block().getAllValues());
        }
        assertFalse(f.entries.containsKey("unexpected"));
        assertEquals(1, f.entries.get("name").version());
        assertEquals(1, f.entries.get("negative").version());
        assertEquals(1, f.entries.get("missing").version());
    }

    @Test
    public void retryAfterPartialBatchDoesNotRetainAbandonedResults() {
        Fixture f = fixture(-1, new InternalEventBus());
        AtomicInteger attempt = new AtomicInteger();
        when(f.backend.get(anyCollection())).thenReturn(Flux.defer(() -> attempt.incrementAndGet() == 1
            ? Flux.concat(Flux.just(new AbstractMap.SimpleImmutableEntry<>("old", "abandoned")),
                          Flux.error(new IllegalStateException("retry")))
            : Flux.just(new AbstractMap.SimpleImmutableEntry<>("name", "current"))));
        Values result = f.storage.getConfigs(Arrays.asList("old", "name")).retry(1).block();
        assertEquals(Collections.singletonMap("name", "current"), result.getAllValues());
        assertEquals(1, f.manager.monitor.loadErrors.sum());
    }

    @Test
    public void downstreamBatchErrorsDoNotCountAsBackendFailures() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get(anyCollection())).thenReturn(Flux.just(
            new AbstractMap.SimpleImmutableEntry<>("name", "value")));
        StepVerifier.create(f.storage.getConfigs(Collections.singleton("name"))
            .map(values -> { throw new IllegalStateException("consumer failed"); }))
            .expectErrorMessage("consumer failed").verify(Duration.ofSeconds(3));
        assertEquals("value", f.storage.getConfig("name").block(Duration.ofSeconds(3)).asString());
        assertEquals(0, f.manager.monitor.loadErrors.sum());
        verify(f.backend, times(1)).get(anyCollection());
    }

    @Test
    public void sharedReadersUseOneBackendLoad() {
        Fixture f = fixture(-1, new InternalEventBus());
        Sinks.One<Object> source = Sinks.one();
        when(f.backend.get("name")).thenReturn(source.asMono());
        CompletableFuture<Value> first = f.storage.getConfig("name").toFuture();
        CompletableFuture<Value> second = f.storage.getConfig("name").toFuture();
        source.emitValue("value", Sinks.EmitFailureHandler.FAIL_FAST);
        assertEquals("value", first.join().asString());
        assertEquals("value", second.join().asString());
        verify(f.backend, times(1)).get("name");
        assertEquals(0, f.manager.monitor.loadErrors.sum());
    }

    @Test
    public void successfulAndEmptyLoadsDoNotReportErrors() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.just("value"));
        when(f.backend.get("missing")).thenReturn(Mono.empty());
        assertEquals("value", f.storage.getConfig("name").block().asString());
        assertNull(f.storage.getConfig("missing").block());
        assertEquals(0, f.manager.monitor.loadErrors.sum());
    }

    @Test
    public void singleLoadWaitsForTheExistingFallback() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.empty());
        Sinks.One<Object> fallback = Sinks.one();
        Mono<Value> read = f.storage.getConfig("name", fallback.asMono());
        CompletableFuture<Value> result = read.toFuture();
        assertFalse(result.isDone());
        fallback.emitValue("value", Sinks.EmitFailureHandler.FAIL_FAST);
        assertEquals("value", result.join().asString());
        assertEquals(0, f.manager.monitor.loadErrors.sum());
        verify(f.backend, times(1)).get("name");
    }

    @Test
    public void callerFallbackFailuresAreNotStorageLoadErrors() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get(anyString())).thenReturn(Mono.empty());
        assertNull(f.storage.getConfig("empty", Mono.empty()).block());
        StepVerifier.create(f.storage.getConfig("error", Mono.error(new IllegalStateException("fallback failed"))))
            .expectErrorMessage("fallback failed")
            .verify();
        assertEquals(0, f.manager.monitor.loadErrors.sum());
    }

    @Test
    public void committedWritePreservesItsValueWhileACallerFallbackFinishes() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.empty());
        when(f.backend.put("name", "new")).thenReturn(Mono.just(true));
        AtomicInteger cancellations = new AtomicInteger();
        Sinks.One<Object> source = Sinks.one();
        CompletableFuture<Value> read = f.storage.getConfig("name", source.asMono().doOnCancel(cancellations::incrementAndGet)).toFuture();
        LocalCacheClusterConfigStorage.Cache entry = f.entries.get("name");
        assertTrue(f.storage.setConfig("name", "new").block());
        assertFalse(read.isDone());
        source.tryEmitValue("old-default");
        assertEquals("old-default", read.orTimeout(3, TimeUnit.SECONDS).join().asString());
        assertEquals("new", entry.getCachedValue());
        assertEquals(0, cancellations.get());
    }

    @Test
    public void cancelledBatchCancelsTheBackendWithoutReportingAnError() {
        Fixture f = fixture(-1, new InternalEventBus());
        AtomicInteger cancellations = new AtomicInteger();
        when(f.backend.get(anyCollection())).thenReturn(
            Flux.<Map.Entry<String, Object>>never().doOnCancel(cancellations::incrementAndGet));
        Disposable reader = f.storage.getConfigs(Collections.singleton("name")).subscribe();
        reader.dispose();
        assertEquals(1, cancellations.get());
        assertEquals(0, f.manager.monitor.loadErrors.sum());
    }

    @Test
    public void staleFallbackIsVisibleInsteadOfHidingAReadFailure() {
        for (boolean hasOldValue : new boolean[]{true, false}) {
            Fixture f = fixture(60_000, new InternalEventBus());
            when(f.backend.get("name")).thenReturn(hasOldValue ? Mono.just("old") : Mono.empty(),
                Mono.error(new IllegalStateException("down")));
            f.storage.getConfig("name").block();
            f.entries.get("name").lastAccessNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(60_001);
            Value result = f.storage.getConfig("name").block();
            if (hasOldValue) {
                assertEquals("old", result.asString());
            } else {
                assertNull(result);
            }
            assertEquals(1, f.manager.monitor.staleFallbacks.sum());
            assertEquals(1, f.manager.monitor.loadErrors.sum());
            assertEquals(IllegalStateException.class.getName(), bean(f).getStatistics().get("lastErrorType"));
            assertEquals("load", bean(f).getStatistics().get("lastErrorStage"));
        }
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
        verify(f.backend, times(1)).put("name", "new");
    }

    @Test
    public void passiveStatisticsAndInspectionDoNotRenewOrLoadCache() {
        Fixture f = fixture(60_000, new InternalEventBus());
        when(f.backend.get(anyCollection())).thenReturn(Flux.just(new AbstractMap.SimpleImmutableEntry<>("name", "value")));
        f.storage.getConfigs(Arrays.asList("name", "missing")).block();
        long oldTime = System.nanoTime() - TimeUnit.SECONDS.toNanos(61);
        f.entries.values().forEach(entry -> entry.lastAccessNanos = oldTime);
        EventBusStorageManager.ConfigStorageManagerMbean bean = bean(f);
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
        f.entries.values().forEach(entry -> assertEquals(oldTime, entry.lastAccessNanos));
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
        assertEquals(0L, bean(f).getStatistics().get("countCapturedAt"));
        for (int i = 0; i < 200 && (long) bean(f).getStatistics().get("countCapturedAt") == 0; i++) {
            f.manager.scanCounts(now + i + 1);
        }
        assertTrue((long) bean(f).getStatistics().get("countCapturedAt") > 0);
        assertEquals(0L, bean(f).getStatistics().get("localEntryCount"));
        assertEquals(70_001L, bean(f).getStatistics().get("storageCount"));
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
        assertEquals(now, bean(f).getStatistics().get("countCapturedAt"));
        f.manager.scanCounts(now + 150_000);
        CompositeData timedOut = bean(f).getStatistics();
        assertEquals(1L, timedOut.get("localEntryCount"));
        assertEquals(now, timedOut.get("countCapturedAt"));
        f.manager.dispose();
        f.manager.scanCounts(now + 180_000);
        assertEquals(now, bean(f).getStatistics().get("countCapturedAt"));
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
        assertFalse(statistics.containsKey("writeSuccesses"));
        assertFalse(statistics.containsKey("writeFailures"));
        assertFalse(statistics.containsKey("notificationsSent"));
        assertFalse(statistics.containsKey("notificationFailures"));
        assertFalse(statistics.containsKey("loadSampleInterval"));
        assertTrue(statistics.containsKey("loadErrors"));
        assertTrue(statistics.containsKey("invalidationErrors"));
        assertTrue(statistics.containsKey("listenerErrors"));
        assertFalse(Arrays.stream(server.getMBeanInfo(name).getAttributes())
            .anyMatch(attribute -> "ReadSamplingEnabled".equals(attribute.getName())));
        assertFalse(Arrays.stream(server.getMBeanInfo(name).getOperations())
            .anyMatch(operation -> "configureReadSampling".equals(operation.getName())));
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

    @Test
    public void localInvalidationErrorsDoNotPreventListenersOrBecomeLoadErrors() {
        Fixture f = fixture(-1, new InternalEventBus());
        LocalCacheClusterConfigStorage failing = spy(f.storage);
        doThrow(new IllegalStateException("invalidation failed")).when(failing).clearLocalCache(any(CacheNotify.class));
        f.manager.cache.put("device:test", failing);
        AtomicInteger notifications = new AtomicInteger();
        f.manager.listenCacheNotify(ignored -> notifications.incrementAndGet());
        f.manager.handleNotify(CacheNotify.expires("device:test", Collections.singleton("name")));
        CompositeData statistics = bean(f).getStatistics();
        assertEquals(1, notifications.get());
        assertEquals(1L, statistics.get("invalidationErrors"));
        assertEquals(0L, statistics.get("listenerErrors"));
        assertEquals(0L, statistics.get("loadErrors"));
        assertEquals("invalidation", statistics.get("lastErrorStage"));
        assertEquals(IllegalStateException.class.getName(), statistics.get("lastErrorType"));
        assertTrue((long) statistics.get("lastErrorAt") > 0);
    }

    @Test
    public void listenerFailureDoesNotMeanLocalInvalidationFailed() {
        Fixture f = fixture(-1, new InternalEventBus());
        when(f.backend.get("name")).thenReturn(Mono.just("old"), Mono.just("new"));
        assertEquals("old", f.storage.getConfig("name").block().asString());
        f.manager.listenCacheNotify(ignored -> {
            throw new IllegalArgumentException("listener failed");
        });
        f.manager.handleNotify(CacheNotify.expires("device:test", Collections.singleton("name")));
        assertEquals("new", f.storage.getConfig("name").block().asString());
        CompositeData statistics = bean(f).getStatistics();
        assertEquals(0L, statistics.get("invalidationErrors"));
        assertEquals(1L, statistics.get("listenerErrors"));
        assertEquals(0L, statistics.get("loadErrors"));
        assertEquals("listener", statistics.get("lastErrorStage"));
        assertEquals(IllegalArgumentException.class.getName(), statistics.get("lastErrorType"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void subscriptionFailureRemainsRetryableAndHasItsOwnErrorStage() {
        EventBus bus = mock(EventBus.class);
        when(bus.subscribe(any(org.jetlinks.core.event.Subscription.class), any(Function.class)))
            .thenThrow(new IllegalStateException("subscribe failed"));
        Fixture f = fixture(-1, bus);
        for (int i = 0; i < 2; i++) {
            try {
                f.manager.getStorage("device:test");
                fail("The subscription failure must reach the caller");
            } catch (IllegalStateException expected) {
                assertEquals("subscribe failed", expected.getMessage());
            }
        }
        verify(bus, times(2)).subscribe(any(org.jetlinks.core.event.Subscription.class), any(Function.class));
        CompositeData statistics = bean(f).getStatistics();
        assertEquals(false, statistics.get("notificationSubscriptionActive"));
        assertEquals("subscription", statistics.get("lastErrorStage"));
        assertEquals(0L, statistics.get("loadErrors"));
        assertEquals(0L, statistics.get("invalidationErrors"));
    }

    @Test
    public void failedCountScanRetainsAnUninitializedSnapshotAndCanRecover() {
        ConcurrentMap<String, Object> entries = spy(new ConcurrentHashMap<>());
        EventBusStorageManager manager = new EventBusStorageManager(
            mock(ClusterManager.class), new InternalEventBus(), () -> entries);
        managers.add(manager);
        doThrow(new IllegalStateException("scan failed")).when(entries).values();
        long now = System.currentTimeMillis();
        manager.scanCounts(now);
        EventBusStorageManager.ConfigStorageManagerMbean bean = manager.new ConfigStorageManagerMbeanImpl();
        assertEquals(-1L, bean.getStatistics().get("localEntryCount"));
        assertEquals(0L, bean.getStatistics().get("countCapturedAt"));
        assertEquals("countScan", bean.getStatistics().get("lastErrorStage"));
        doCallRealMethod().when(entries).values();
        manager.scanCounts(now + 30_000);
        assertEquals(0L, bean.getStatistics().get("localEntryCount"));
        assertEquals(now + 30_000, bean.getStatistics().get("countCapturedAt"));
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
