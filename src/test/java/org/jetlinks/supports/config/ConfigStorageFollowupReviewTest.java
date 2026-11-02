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



    @Test public void finishingAnOldLoadDoesNotAffectANewStorageOrItsLifetime() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Object> old = Sinks.one(), fresh = Sinks.one();
            when(f.backend.get("old")).thenReturn(old.asMono());
            when(f.backend.get("new")).thenReturn(fresh.asMono());
            CompletableFuture<Value> first = f.storage.getConfig("old").toFuture();
            f.storage.clearLocalCache(CacheNotify.clear("audit"));
            f.manager.cleanup();
            LocalCacheClusterConfigStorage replacement = new LocalCacheClusterConfigStorage(
                "audit", f.manager, f.backend, -1, null);
            f.manager.cache.put("audit", replacement);
            CompletableFuture<Value> second = replacement.getConfig("new").toFuture();
            old.tryEmitValue("old-value");
            assertEquals("old-value", first.join().asString());
            assertFalse(second.isDone());
            f.manager.dispose();
            assertTrue(f.manager.cache.isEmpty());
            assertFalse(second.isDone());
            fresh.tryEmitValue("new-value");
            assertEquals("new-value", second.orTimeout(3, TimeUnit.SECONDS).join().asString());
            StepVerifier.create(replacement.getConfig("closed"))
                .expectError(CancellationException.class).verify(Duration.ofSeconds(3));
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

    @Test public void shutdownDoesNotOwnAlreadyStartedIndependentReads() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Object> singleSource = Sinks.one();
            Sinks.One<Map.Entry<String, Object>> batchSource = Sinks.one();
            when(f.backend.get("a")).thenReturn(singleSource.asMono());
            when(f.backend.get(anyCollection())).thenReturn(batchSource.asMono().flux());
            CompletableFuture<Value> single = f.storage.getConfig("a").toFuture();
            CompletableFuture<Values> batch = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            f.manager.dispose();
            assertFalse(single.isDone()); assertFalse(batch.isDone());
            singleSource.tryEmitValue("A");
            batchSource.tryEmitValue(new AbstractMap.SimpleImmutableEntry<>("b", "B"));
            assertEquals("A", single.orTimeout(3, TimeUnit.SECONDS).join().asString());
            assertEquals("B", batch.orTimeout(3, TimeUnit.SECONDS).join().getString("b", ""));
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

    @Test public void batchCollectionFailureBeforeTheFirstTargetPropagatesAndAllowsRetry() {
        AtomicBoolean first = new AtomicBoolean(true);
        Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<String, LocalCacheClusterConfigStorage.Cache>() {
            @Override public LocalCacheClusterConfigStorage.Cache computeIfAbsent(
                String key, java.util.function.Function<? super String, ? extends LocalCacheClusterConfigStorage.Cache> factory) {
                if (first.compareAndSet(true, false)) throw new IllegalStateException("collection failed");
                return super.computeIfAbsent(key, factory);
            }
        };
        try (Fixture f = new Fixture(entries)) {
            when(f.backend.get(anyCollection())).thenReturn(Flux.just(new AbstractMap.SimpleImmutableEntry<>("a", "A")));
            Mono<Values> read = f.storage.getConfigs(Arrays.asList("a", "b"));
            StepVerifier.create(read).expectErrorMessage("collection failed").verify(Duration.ofSeconds(3));
            assertEquals("A", read.block(Duration.ofSeconds(3)).getString("a", ""));
            verify(f.backend, times(1)).get(anyCollection());
        }
    }

    @Test public void immediateBatchCancellationSkipsL2WhileASharedFillCanFinish() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Object> source = Sinks.one();
            when(f.backend.get("key")).thenReturn(source.asMono());
            AtomicReference<Throwable> dropped = new AtomicReference<>();
            reactor.core.publisher.Hooks.onErrorDropped(dropped::set);
            try {
                f.storage.getConfig("key").doOnSubscribe(org.reactivestreams.Subscription::cancel).subscribe();
                f.storage.getConfigs(Arrays.asList("a", "b"))
                    .doOnSubscribe(org.reactivestreams.Subscription::cancel).subscribe();
                source.tryEmitValue("filled");
                assertEquals("filled", f.storage.getConfig("key").block(Duration.ofSeconds(3)).asString());
                assertNull(dropped.get());
            } finally { reactor.core.publisher.Hooks.resetOnErrorDropped(); }
            verify(f.backend, times(1)).get("key");
            verify(f.backend, never()).get(anyCollection());
        }
    }

    @Test public void singlesAndBatchesKeepTheirOwnContextDuringCollection() throws Exception {
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
        AtomicInteger subscriptions = new AtomicInteger();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Fixture f = new Fixture(entries)) {
            when(f.backend.get("a")).thenReturn(Mono.deferContextual(context -> Mono.just(context.get("driver"))));
            when(f.backend.get(anyCollection())).thenReturn(Flux.deferContextual(context -> {
                subscriptions.incrementAndGet();
                return Flux.just(new AbstractMap.SimpleImmutableEntry<>("a", context.get("driver")),
                    new AbstractMap.SimpleImmutableEntry<>("b", context.get("driver")));
            }));
            Future<CompletableFuture<Values>> owner = executor.submit(() -> f.storage.getConfigs(Arrays.asList("a", "b"))
                .contextWrite(context -> context.put("driver", "owner")).toFuture());
            try {
                assertTrue(paused.await(2, TimeUnit.SECONDS));
                CompletableFuture<Value> joined = f.storage.getConfig("a")
                    .contextWrite(context -> context.put("driver", "joined")).toFuture();
                assertTrue(joined.isDone());
                assertEquals(0, subscriptions.get());
                resume.countDown();
                assertEquals("joined", joined.get(2, TimeUnit.SECONDS).asString());
                assertEquals("owner", owner.get(2, TimeUnit.SECONDS).get(2, TimeUnit.SECONDS).getString("b", ""));
                assertEquals(1, subscriptions.get());
            } finally { resume.countDown(); }
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test public void aColdReadRacingEmptyCleanupRemainsRegisteredForInvalidation() throws Exception {
        CountDownLatch removing = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch reading = new CountDownLatch(1);
        AtomicInteger emptyChecks = new AtomicInteger();
        AtomicInteger keyReads = new AtomicInteger();
        Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<String, LocalCacheClusterConfigStorage.Cache>() {
            @Override public boolean isEmpty() {
                boolean empty = super.isEmpty();
                if (empty && emptyChecks.incrementAndGet() == 2) {
                    removing.countDown();
                    try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                    catch (InterruptedException error) { throw new AssertionError(error); }
                }
                return empty;
            }
            @Override public LocalCacheClusterConfigStorage.Cache get(Object key) {
                if ("key".equals(key) && keyReads.incrementAndGet() == 2) reading.countDown();
                return super.get(key);
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Fixture f = new Fixture(entries)) {
            when(f.backend.get("key")).thenReturn(Mono.just("first"), Mono.just("second"));
            Future<?> cleanup = executor.submit(f.manager::cleanup);
            try {
                assertTrue(removing.await(2, TimeUnit.SECONDS));
                Future<Value> read = executor.submit(() -> f.storage.getConfig("key").block(Duration.ofSeconds(3)));
                assertTrue(reading.await(2, TimeUnit.SECONDS));
                resume.countDown();
                cleanup.get(2, TimeUnit.SECONDS);
                assertEquals("first", read.get(2, TimeUnit.SECONDS).asString());
                assertSame(f.storage, f.manager.cache.get("audit"));
                f.manager.handleNotify(CacheNotify.expires("audit", Arrays.asList("key")));
                assertEquals("second", f.storage.getConfig("key").block(Duration.ofSeconds(3)).asString());
            } finally { resume.countDown(); }
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test public void shutdownLeavesAnAlreadyClaimedSourceToComplete() throws Exception {
        CountDownLatch creating = new CountDownLatch(1), resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Fixture f = new Fixture()) {
            Sinks.One<Object> source = Sinks.one();
            when(f.backend.get("key")).thenAnswer(ignored -> {
                creating.countDown(); assertTrue(resume.await(3, TimeUnit.SECONDS));
                return source.asMono();
            });
            Future<CompletableFuture<Value>> owner = executor.submit(() -> f.storage.getConfig("key").toFuture());
            try {
                assertTrue(creating.await(2, TimeUnit.SECONDS));
                CompletableFuture<Value> joined = f.storage.getConfig("key").toFuture();
                f.manager.dispose(); assertFalse(joined.isDone()); resume.countDown();
                CompletableFuture<Value> first = owner.get(2, TimeUnit.SECONDS);
                source.tryEmitValue("finished");
                assertEquals("finished", first.get(2, TimeUnit.SECONDS).asString());
                assertEquals("finished", joined.get(2, TimeUnit.SECONDS).asString());
                assertEquals(0, f.manager.monitor.loadErrors.sum());
            } finally { resume.countDown(); }
        } finally { resume.countDown(); executor.shutdownNow(); }
    }

    @Test public void aRepeatedKeyDoesNotKeepAnEarlierSeedAfterInvalidation() {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<String, LocalCacheClusterConfigStorage.Cache>() {
            @Override public LocalCacheClusterConfigStorage.Cache get(Object key) {
                if (armed.get() && "a".equals(key) && reads.incrementAndGet() == 2) remove(key);
                return super.get(key);
            }
        };
        try (Fixture f = new Fixture(entries)) {
            when(f.backend.get("a")).thenReturn(Mono.just("old"));
            assertEquals("old", f.storage.getConfig("a").block(Duration.ofSeconds(3)).asString());
            when(f.backend.get(anyCollection())).thenReturn(Flux.just(new AbstractMap.SimpleImmutableEntry<>("a", "fresh")));
            armed.set(true);
            assertEquals("fresh", f.storage.getConfigs(Arrays.asList("missing", "a", "a"))
                .block(Duration.ofSeconds(3)).getString("a", ""));
            assertEquals("fresh", f.storage.getConfig("a").block(Duration.ofSeconds(3)).asString());
        }
    }

    @Test public void shutdownDuringColdRegistrationCannotReattachTheStorage() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), resume = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<String, LocalCacheClusterConfigStorage.Cache>() {
            @Override public LocalCacheClusterConfigStorage.Cache get(Object key) {
                if ("a".equals(key) && reads.incrementAndGet() == 2) {
                    entered.countDown();
                    try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                    catch (InterruptedException error) { throw new AssertionError(error); }
                }
                return super.get(key);
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Fixture f = new Fixture(entries)) {
            Future<CompletableFuture<Values>> task = executor.submit(() -> f.storage.getConfigs(Arrays.asList("a", "b")).toFuture());
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            f.manager.dispose(); resume.countDown();
            StepVerifier.create(Mono.fromFuture(task.get(2, TimeUnit.SECONDS)))
                .expectError(CancellationException.class).verify(Duration.ofSeconds(3));
            assertTrue(f.manager.cache.isEmpty());
            verifyNoInteractions(f.backend);
        } finally { resume.countDown(); executor.shutdownNow(); }
    }

    @Test public void invalidationBetweenNegativeAndFallbackCannotSeedTheReplacement() {
        try (Fixture f = new Fixture()) {
            Sinks.Empty<Object> source = Sinks.empty();
            when(f.backend.get("key")).thenReturn(source.asMono(), Mono.just("fresh"));
            CompletableFuture<Value> ordinary = f.storage.getConfig("key")
                .doOnSuccess(ignored -> f.storage.clearLocalCache(CacheNotify.expiresAll("audit"))).toFuture();
            CompletableFuture<Value> fallback = f.storage.getConfig("key", Mono.just("old-default")).toFuture();
            source.tryEmitEmpty();
            assertNull(ordinary.orTimeout(3, TimeUnit.SECONDS).join());
            assertEquals("old-default", fallback.orTimeout(3, TimeUnit.SECONDS).join().asString());
            assertEquals("fresh", f.storage.getConfig("key").block(Duration.ofSeconds(3)).asString());
            verify(f.backend, times(2)).get("key");
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
            assertEquals(0, f.manager.monitor.loadErrors.sum());
        }
    }

    @Test public void aSuccessfulFallbackCachesWithoutEndingAnotherCallersSource() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            AtomicInteger cancellations = new AtomicInteger();
            CompletableFuture<Value> first = f.storage.getConfig("key",
                Mono.never().doOnCancel(cancellations::incrementAndGet)).toFuture();
            assertEquals("created", f.storage.getConfig("key", Mono.just("created"))
                .block(Duration.ofSeconds(3)).asString());
            assertFalse(first.isDone()); assertEquals(0, cancellations.get());
            assertEquals("created", f.storage.getConfig("key").block(Duration.ofSeconds(3)).asString());
            first.cancel(true); assertEquals(1, cancellations.get());
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
            f.entries.get("key").lastAccessNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(60_001);
            Value value = f.storage.getConfig("key", Mono.just("default")).block(Duration.ofSeconds(3));
            assertEquals("default", value.asString());
            verify(f.backend, times(2)).get("key");
            assertEquals(1, f.manager.monitor.loadErrors.sum());
            assertEquals(1, f.manager.monitor.staleFallbacks.sum());
        }
    }

    @Test public void detachedFallbackRemainsOwnedByItsCallerAfterShutdown() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            AtomicInteger cancellations = new AtomicInteger();
            CompletableFuture<Value> read = f.storage.getConfig("key",
                Mono.never().doOnCancel(cancellations::incrementAndGet)).toFuture();
            f.storage.clearLocalCache(CacheNotify.clear("audit")); f.manager.dispose();
            assertFalse(read.isDone()); assertEquals(0, cancellations.get());
            read.cancel(true); assertEquals(1, cancellations.get());
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
                assertFalse(read.isDone());
            } finally { resume.countDown(); }
            delivery.get(2, TimeUnit.SECONDS);
            assertSame(older, read.get(2, TimeUnit.SECONDS).get());
            assertEquals("newer", cache.getCachedValue());
            assertEquals(2, cache.version());
        }
    }

    @Test public void aWriteCompletesASharedSingleButBatchesAwaitTheirOwnL2() {
        try (Fixture f = new Fixture()) {
            AtomicInteger cancellations = new AtomicInteger();
            Sinks.One<Map.Entry<String, Object>> source = Sinks.one();
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flux().doOnCancel(cancellations::incrementAndGet));
            when(f.backend.put("a", "written")).thenReturn(Mono.just(true));
            when(f.backend.get("a")).thenReturn(Mono.never());
            CompletableFuture<Values> batch = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            CompletableFuture<Value> joined = f.storage.getConfig("a").toFuture();
            CompletableFuture<Values> joinedBatch = f.storage.getConfigs(java.util.Collections.singleton("a")).toFuture();
            f.storage.setConfig("a", "written").block(Duration.ofSeconds(3));
            assertEquals("written", joined.join().asString());
            assertFalse(joinedBatch.isDone());
            assertFalse(batch.isDone());
            assertEquals(0, cancellations.get());
            source.tryEmitValue(new AbstractMap.SimpleImmutableEntry<>("b", "B"));
            assertEquals("written", joinedBatch.orTimeout(3, TimeUnit.SECONDS).join().getString("a", ""));
            Values values = batch.join();
            assertEquals("written", values.getString("a", ""));
            assertEquals("B", values.getString("b", ""));
        }
    }

    @Test public void writesDuringCollectionPreserveNewValuesWhenTheBatchFinishes() throws Exception {
        CountDownLatch collecting = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<String, LocalCacheClusterConfigStorage.Cache>() {
            @Override public LocalCacheClusterConfigStorage.Cache computeIfAbsent(
                String key, java.util.function.Function<? super String, ? extends LocalCacheClusterConfigStorage.Cache> factory) {
                LocalCacheClusterConfigStorage.Cache cache = super.computeIfAbsent(key, factory);
                if ("b".equals(key)) {
                    collecting.countDown();
                    try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                    catch (InterruptedException error) { throw new AssertionError(error); }
                }
                return cache;
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Fixture f = new Fixture(entries)) {
            Map<String, Object> written = new java.util.LinkedHashMap<>();
            written.put("a", "A"); written.put("b", "B");
            when(f.backend.putAll(written)).thenReturn(Mono.just(true));
            when(f.backend.get(anyCollection())).thenReturn(Flux.empty());
            Future<CompletableFuture<Values>> owner = executor.submit(() -> f.storage.getConfigs(Arrays.asList("a", "b")).toFuture());
            try {
                assertTrue(collecting.await(2, TimeUnit.SECONDS));
                assertTrue(f.storage.setConfigs(written).block(Duration.ofSeconds(3)));
                resume.countDown();
                Values values = owner.get(2, TimeUnit.SECONDS).get(2, TimeUnit.SECONDS);
                assertEquals("A", values.getString("a", ""));
                assertEquals("B", values.getString("b", ""));
                verify(f.backend, times(1)).get(anyCollection());
            } finally { resume.countDown(); }
        } finally { executor.shutdownNow(); }
    }

    @Test public void aBatchFailureAfterOneWrittenKeyStillFailsAndCanRetry() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Map.Entry<String, Object>> source = Sinks.one();
            IllegalStateException failure = new IllegalStateException("batch failed");
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flux());
            when(f.backend.put("a", "written")).thenReturn(Mono.just(true));
            CompletableFuture<Values> batch = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            f.storage.setConfig("a", "written").block(Duration.ofSeconds(3));
            source.tryEmitError(failure);
            try { batch.join(); fail("Unresolved keys must preserve the backend failure"); }
            catch (CompletionException error) { assertSame(failure, error.getCause()); }
            when(f.backend.get(anyCollection())).thenReturn(Flux.just(
                new AbstractMap.SimpleImmutableEntry<>("a", "written"),
                new AbstractMap.SimpleImmutableEntry<>("b", "retry")));
            assertEquals("retry", f.storage.getConfigs(Arrays.asList("a", "b"))
                .block(Duration.ofSeconds(3)).getString("b", ""));
        }
    }

    @Test public void aFailedSingleDoesNotFailAnIndependentBatch() {
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
            assertFalse(batch.isDone());
            batch.cancel(true);
            assertEquals(1, bCancellations.get());
            when(f.backend.get("b")).thenReturn(Mono.just("retry"));
            assertEquals("retry", f.storage.getConfig("b").block(Duration.ofSeconds(3)).asString());
        }
    }

    @Test public void aCompletedBatchStillWaitsForDownstreamDemand() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get(anyCollection())).thenReturn(Flux.just(
                new AbstractMap.SimpleImmutableEntry<>("a", "A"),
                new AbstractMap.SimpleImmutableEntry<>("b", "B")));
            AtomicReference<Values> result = new AtomicReference<>();
            AtomicBoolean complete = new AtomicBoolean();
            reactor.core.publisher.BaseSubscriber<Values> reader = new reactor.core.publisher.BaseSubscriber<Values>() {
                @Override protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) { }
                @Override protected void hookOnNext(Values values) { result.set(values); }
                @Override protected void hookOnComplete() { complete.set(true); }
            };
            f.storage.getConfigs(Arrays.asList("a", "b")).subscribe(reader);
            verify(f.backend, times(1)).get(anyCollection());
            assertNull(result.get());
            assertFalse(complete.get());
            reader.request(1);
            assertEquals("A", result.get().getString("a", ""));
            assertEquals("B", result.get().getString("b", ""));
            assertTrue(complete.get());
        }
    }

    @Test public void cancellingOneOverlappingBatchLeavesTheOtherRequestIndependent() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Map.Entry<String, Object>> first = Sinks.one();
            Sinks.One<Map.Entry<String, Object>> second = Sinks.one();
            AtomicInteger firstCancelled = new AtomicInteger();
            AtomicInteger secondCancelled = new AtomicInteger();
            when(f.backend.get(anyCollection())).thenReturn(
                first.asMono().flux().doOnCancel(firstCancelled::incrementAndGet),
                second.asMono().flux().doOnCancel(secondCancelled::incrementAndGet));
            CompletableFuture<Values> abandoned = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            CompletableFuture<Values> active = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            abandoned.cancel(true);
            assertEquals(1, firstCancelled.get());
            assertEquals(0, secondCancelled.get());
            assertFalse(active.isDone());
            second.tryEmitValue(new AbstractMap.SimpleImmutableEntry<>("a", "A"));
            assertEquals("A", active.orTimeout(3, TimeUnit.SECONDS).join().getString("a", ""));
        }
    }

    @Test public void shutdownLeavesABatchIndependentOfAPausedSingleDelivery() throws Exception {
        CountDownLatch receiving = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Fixture f = new Fixture()) {
            Sinks.One<Map<String, Object>> source = Sinks.one();
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flatMapMany(values ->
                Flux.fromIterable(Arrays.asList("a", "b"))
                    .map(key -> new AbstractMap.SimpleImmutableEntry<>(key, values.get(key)))));
            Sinks.One<Object> singleSource = Sinks.one();
            when(f.backend.get("a")).thenReturn(singleSource.asMono());
            CompletableFuture<Values> batch = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            CompletableFuture<Value> joined = f.storage.getConfig("a").doOnNext(value -> {
                receiving.countDown();
                try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException error) { throw new AssertionError(error); }
            }).toFuture();
            Map<String, Object> values = new java.util.LinkedHashMap<>();
            values.put("a", "A"); values.put("b", "B");
            Future<?> delivery = executor.submit(() -> singleSource.tryEmitValue("A"));
            try {
                assertTrue(receiving.await(2, TimeUnit.SECONDS));
                f.manager.dispose();
                assertFalse(batch.isDone());
                source.tryEmitValue(values);
                assertEquals("B", batch.get(2, TimeUnit.SECONDS).getString("b", ""));
            } finally { resume.countDown(); }
            delivery.get(2, TimeUnit.SECONDS);
            assertEquals("A", joined.get(2, TimeUnit.SECONDS).asString());
        } finally { resume.countDown(); executor.shutdownNow(); }
    }

    @Test public void simultaneousIndependentBatchesAllFinish() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        try (Fixture f = new Fixture()) {
            Sinks.One<Map<String, Object>> source = Sinks.one();
            Map<String, Object> expected = new java.util.LinkedHashMap<>();
            for (int i = 0; i < 8; i++) expected.put("key-" + i, "value-" + i);
            Map<String, Integer> requests = new ConcurrentHashMap<>();
            when(f.backend.get(anyCollection())).thenAnswer(call -> {
                java.util.List<String> keys = new java.util.ArrayList<>(call.getArgument(0));
                keys.forEach(key -> requests.merge(key, 1, Integer::sum));
                return source.asMono().flatMapMany(values -> Flux.fromIterable(keys)
                    .map(key -> new AbstractMap.SimpleImmutableEntry<>(key, values.get(key))));
            });
            java.util.List<Future<CompletableFuture<Values>>> tasks = new java.util.ArrayList<>();
            for (int i = 0; i < 16; i++) tasks.add(executor.submit(() -> {
                assertTrue(start.await(2, TimeUnit.SECONDS));
                return f.storage.getConfigs(expected.keySet()).toFuture();
            }));
            start.countDown();
            java.util.List<CompletableFuture<Values>> readers = new java.util.ArrayList<>();
            for (Future<CompletableFuture<Values>> task : tasks) readers.add(task.get(3, TimeUnit.SECONDS));
            assertEquals(8, requests.size());
            requests.values().forEach(count -> assertEquals(Integer.valueOf(16), count));
            source.tryEmitValue(expected);
            for (CompletableFuture<Values> reader : readers)
                assertEquals(expected, reader.get(3, TimeUnit.SECONDS).getAllValues());
        } finally { start.countDown(); executor.shutdownNow(); }
    }
}
