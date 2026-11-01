package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/** Regressions for subscriber-local defaults, construction races and lazy cache registration. */
public class ConfigStorageFollowupReviewTest {
    private static class Fixture implements AutoCloseable {
        final ClusterCache<String, Object> backend = mock(ClusterCache.class);
        final EventBusStorageManager manager = new EventBusStorageManager(mock(ClusterManager.class), new InternalEventBus());
        final Map<String, LocalCacheClusterConfigStorage.Cache> entries;
        final LocalCacheClusterConfigStorage storage;
        Fixture() { this(new ConcurrentHashMap<>()); }
        Fixture(long ttl) { this(new ConcurrentHashMap<>(), ttl); }
        Fixture(Map<String, LocalCacheClusterConfigStorage.Cache> entries) { this(entries, -1); }
        Fixture(Map<String, LocalCacheClusterConfigStorage.Cache> entries, long ttl) {
            this.entries = entries;
            storage = new LocalCacheClusterConfigStorage("audit", manager, backend, ttl, null, entries);
            manager.cache.put("audit", storage);
        }
        @Override public void close() { manager.dispose(); }
    }

    @Test public void ordinaryReaderMustNotWaitForAnotherCallersFallback() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            CompletableFuture<Value> owner = f.storage.getConfig("key", Mono.never()).toFuture();
            CompletableFuture<Value> ordinary = f.storage.getConfig("key").toFuture();
            try {
                assertTrue("L2 already completed empty; ordinary read must not await a caller fallback", ordinary.isDone());
                assertNull(ordinary.join());
            } finally {
                ordinary.cancel(true);
                owner.cancel(true);
            }
        }
    }

    @Test public void aSecondExplicitFallbackMustRunAfterTheFirstReturnsEmpty() throws Exception {
        try (Fixture f = new Fixture()) {
            Sinks.Empty<Object> source = Sinks.empty();
            when(f.backend.get("key")).thenReturn(source.asMono());
            CompletableFuture<Value> first = f.storage.getConfig("key", Mono.empty()).toFuture();
            CompletableFuture<Value> second = f.storage.getConfig("key", Mono.just("second")).toFuture();
            source.tryEmitEmpty();
            assertNull(first.get(1, TimeUnit.SECONDS));
            Value result = second.get(1, TimeUnit.SECONDS);
            assertNotNull("Second fallback was silently skipped", result);
            assertEquals("second", result.asString());
        }
    }

    @Test public void disposeMustEndAJoinedReaderEvenBeforeTheOwningBatchSeals() throws Exception {
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(true);
        Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<String, LocalCacheClusterConfigStorage.Cache>() {
            @Override public LocalCacheClusterConfigStorage.Cache get(Object key) {
                if ("b".equals(key) && armed.compareAndSet(true, false)) {
                    paused.countDown();
                    try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                    catch (InterruptedException error) { throw new AssertionError(error); }
                }
                return super.get(key);
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Fixture f = new Fixture(entries)) {
            Future<CompletableFuture<Values>> batch = executor.submit(() -> f.storage.getConfigs(Arrays.asList("a", "b")).toFuture());
            try {
                assertTrue(paused.await(2, TimeUnit.SECONDS));
                CompletableFuture<Value> joined = f.storage.getConfig("a").toFuture();
                f.manager.dispose();
                assertTrue("Joined reader was not registered for shutdown before ready gate", joined.isDone());
                assertTrue(joined.isCompletedExceptionally());
            } finally {
                resume.countDown();
                batch.get(2, TimeUnit.SECONDS);
            }
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test public void unSubscribedColdReadsShouldNotPopulateTheCache() {
        try (Fixture f = new Fixture()) {
            for (int i = 0; i < 1000; i++) f.storage.getConfig("unsubscribed-" + i);
            f.storage.getConfigs(Arrays.asList("a", "b", "c"));
            verifyNoInteractions(f.backend);
            assertEquals("Unsubscribed publishers created persistent empty entries", 0, f.storage.localEntryCount());
        }
    }

    @Test public void eachFallbackKeepsItsOwnContextAndCancellation() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            AtomicReference<String> firstContext = new AtomicReference<>();
            AtomicReference<String> secondContext = new AtomicReference<>();
            AtomicInteger firstCancels = new AtomicInteger();
            Sinks.Empty<Object> secondSource = Sinks.empty();
            CompletableFuture<Value> first = f.storage.getConfig("key", Mono.deferContextual(ctx -> {
                firstContext.set(ctx.get("trace"));
                return Mono.never().doOnCancel(firstCancels::incrementAndGet);
            })).contextWrite(ctx -> ctx.put("trace", "first")).toFuture();
            CompletableFuture<Value> second = f.storage.getConfig("key", Mono.deferContextual(ctx -> {
                secondContext.set(ctx.get("trace"));
                return secondSource.asMono();
            })).contextWrite(ctx -> ctx.put("trace", "second")).toFuture();
            assertEquals("first", firstContext.get());
            assertEquals("second", secondContext.get());
            first.cancel(true);
            assertEquals(1, firstCancels.get());
            assertFalse(second.isDone());
            secondSource.tryEmitEmpty();
            assertNull(second.join());
            assertEquals(0, f.manager.monitor.loadErrors.sum());
            verify(f.backend, times(1)).get("key");
        }
    }

    @Test public void aFallbackErrorDoesNotFailAnotherCallersLoader() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            Sinks.One<Object> firstSource = Sinks.one();
            Sinks.One<Object> secondSource = Sinks.one();
            CompletableFuture<Value> first = f.storage.getConfig("key", firstSource.asMono()).toFuture();
            CompletableFuture<Value> second = f.storage.getConfig("key", secondSource.asMono()).toFuture();
            firstSource.tryEmitError(new IllegalStateException("first failed"));
            StepVerifier.create(Mono.fromFuture(first)).expectErrorMessage("first failed").verify(Duration.ofSeconds(3));
            assertFalse(second.isDone());
            secondSource.tryEmitValue("second");
            assertEquals("second", second.join().asString());
            assertEquals(1, f.manager.monitor.loadErrors.sum());
        }
    }

    @Test public void aSuccessfulFallbackPublishesOnceAndEndsOtherWaiters() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            AtomicInteger cancellations = new AtomicInteger();
            CompletableFuture<Value> first = f.storage.getConfig("key",
                Mono.never().doOnCancel(cancellations::incrementAndGet)).toFuture();
            CompletableFuture<Value> second = f.storage.getConfig("key", Mono.just("created")).toFuture();
            assertEquals("created", first.join().asString());
            assertEquals("created", second.join().asString());
            assertEquals("created", f.storage.getConfig("key").block(Duration.ofSeconds(3)).asString());
            assertEquals(1, cancellations.get());
            assertEquals(2, f.entries.get("key").version());
            verify(f.backend, times(1)).get("key");
        }
    }

    @Test public void retryAfterFallbackErrorRebuildsOnlyTheCallerLoader() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            AtomicInteger attempts = new AtomicInteger();
            Value value = f.storage.getConfig("key", Mono.defer(() -> attempts.incrementAndGet() == 1
                ? Mono.error(new IllegalStateException("transient")) : Mono.just("recovered")))
                .retry(1).block(Duration.ofSeconds(3));
            assertEquals("recovered", value.asString());
            assertEquals(2, attempts.get());
            verify(f.backend, times(1)).get("key");
        }
    }

    @Test public void anExpiredNegativeAfterL2FailureRunsTheDefaultWithoutRetryingL2() {
        try (Fixture f = new Fixture(60_000)) {
            when(f.backend.get("key")).thenReturn(Mono.empty(), Mono.error(new IllegalStateException("L2 down")));
            assertNull(f.storage.getConfig("key").block(Duration.ofSeconds(3)));
            f.entries.get("key").t = System.currentTimeMillis() - 60_001;
            Value value = f.storage.getConfig("key", Mono.just("default")).block(Duration.ofSeconds(3));
            assertEquals("default", value.asString());
            verify(f.backend, times(2)).get("key");
            assertEquals(1, f.manager.monitor.loadErrors.sum());
            assertEquals(1, f.manager.monitor.staleFallbacks.sum());
        }
    }

    @Test public void shutdownEndsADetachedFallbackAndCancelsItsSource() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            AtomicInteger cancellations = new AtomicInteger();
            StepVerifier.create(f.storage.getConfig("key", Mono.never().doOnCancel(cancellations::incrementAndGet)))
                .then(() -> f.storage.clearLocalCache(CacheNotify.clear("audit")))
                .then(f.manager::dispose).expectError(CancellationException.class).verify(Duration.ofSeconds(3));
            assertEquals(1, cancellations.get());
            assertEquals(0, f.manager.monitor.loadErrors.sum());
        }
    }

    @Test public void anInvalidatedFallbackCannotPopulateTheReplacementCache() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty(), Mono.just("fresh"));
            Sinks.One<Object> source = Sinks.one();
            CompletableFuture<Value> old = f.storage.getConfig("key", source.asMono()).toFuture();
            f.storage.clearLocalCache(CacheNotify.expiresAll("audit"));
            assertEquals("fresh", f.storage.getConfig("key").block(Duration.ofSeconds(3)).asString());
            source.tryEmitValue("old");
            assertEquals("old", old.join().asString());
            assertEquals("fresh", f.storage.getConfig("key").block(Duration.ofSeconds(3)).asString());
        }
    }

    @Test public void aWriteWinsOverAFallbackPausedWhileSharingItsResult() throws Exception {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("productName")).thenReturn(Mono.empty());
            when(f.backend.put("productName", "newer")).thenReturn(Mono.just(true));
            CountDownLatch hashing = new CountDownLatch(1);
            CountDownLatch resume = new CountDownLatch(1);
            AtomicBoolean once = new AtomicBoolean(true);
            Object older = new Object() {
                @Override public int hashCode() {
                    if (once.compareAndSet(true, false)) {
                        hashing.countDown();
                        try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                        catch (InterruptedException error) { throw new AssertionError(error); }
                    }
                    return 493761;
                }
            };
            Sinks.One<Object> source = Sinks.one();
            CompletableFuture<Value> read = f.storage.getConfig("productName", source.asMono()).toFuture();
            LocalCacheClusterConfigStorage.Cache cache = f.entries.get("productName");
            CompletableFuture<Void> delivery = CompletableFuture.runAsync(() -> source.tryEmitValue(older));
            try {
                assertTrue(hashing.await(2, TimeUnit.SECONDS));
                f.storage.setConfig("productName", "newer").block(Duration.ofSeconds(3));
                assertEquals("newer", read.get(2, TimeUnit.SECONDS).asString());
            } finally { resume.countDown(); }
            delivery.get(2, TimeUnit.SECONDS);
            assertEquals("newer", cache.getCachedValue());
            assertEquals(2, cache.version());
        }
    }

    @Test public void aWrittenKeyEndsItsJoinedReaderBeforeTheRemainingBatchCompletes() {
        try (Fixture f = new Fixture()) {
            AtomicInteger cancellations = new AtomicInteger();
            Sinks.One<Map.Entry<String, Object>> source = Sinks.one();
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flux().doOnCancel(cancellations::incrementAndGet));
            when(f.backend.put("a", "written")).thenReturn(Mono.just(true));
            CompletableFuture<Values> batch = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            CompletableFuture<Value> joined = f.storage.getConfig("a").toFuture();
            f.storage.setConfig("a", "written").block(Duration.ofSeconds(3));
            assertEquals("written", joined.join().asString());
            assertFalse(batch.isDone());
            assertEquals(0, cancellations.get());
            source.tryEmitValue(new AbstractMap.SimpleImmutableEntry<>("b", "B"));
            Values values = batch.join();
            assertEquals("written", values.getString("a", ""));
            assertEquals("B", values.getString("b", ""));
        }
    }

    @Test public void aFailedJoinedGroupReleasesNewlyOwnedWork() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Object> a = Sinks.one();
            AtomicInteger bCancellations = new AtomicInteger();
            when(f.backend.get("a")).thenReturn(a.asMono());
            when(f.backend.get(anyCollection())).thenReturn(Flux.<Map.Entry<String, Object>>never()
                .doOnCancel(bCancellations::incrementAndGet));
            CompletableFuture<Value> first = f.storage.getConfig("a").toFuture();
            CompletableFuture<Values> batch = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            a.tryEmitError(new IllegalStateException("failed"));
            StepVerifier.create(Mono.fromFuture(first)).expectErrorMessage("failed").verify(Duration.ofSeconds(3));
            StepVerifier.create(Mono.fromFuture(batch)).expectErrorMessage("failed").verify(Duration.ofSeconds(3));
            assertEquals(1, bCancellations.get());
            when(f.backend.get("b")).thenReturn(Mono.just("retry"));
            assertEquals("retry", f.storage.getConfig("b").block(Duration.ofSeconds(3)).asString());
        }
    }
}
