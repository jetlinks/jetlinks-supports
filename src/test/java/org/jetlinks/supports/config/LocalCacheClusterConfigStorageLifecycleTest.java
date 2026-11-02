package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Deterministic ownership, reactive composition and terminal-state regressions. */
public class LocalCacheClusterConfigStorageLifecycleTest {
    private static final Duration LIMIT = Duration.ofSeconds(3);

    private static class Fixture implements AutoCloseable {
        @SuppressWarnings("unchecked")
        final ClusterCache<String, Object> backend = mock(ClusterCache.class);
        final EventBusStorageManager manager = new EventBusStorageManager(mock(ClusterManager.class), new InternalEventBus());
        final Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<>();
        final LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage("lifecycle", manager, backend,
            -1, s -> manager.cache.computeIfPresent(s.id, (id, current) -> current == s && s.isEmpty() ? null : current), entries);
        Fixture() { manager.cache.put("lifecycle", storage); }
        @Override public void close() { manager.dispose(); }
    }

    private static Map.Entry<String, Object> entry(String key, Object value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    @Test public void coldSingleStartsAtSubscriptionAndRetainsContext() {
        try (Fixture f = new Fixture()) {
            AtomicInteger subscriptions = new AtomicInteger();
            when(f.backend.get("key")).thenReturn(Mono.deferContextual(ctx -> {
                subscriptions.incrementAndGet();
                return Mono.just(ctx.get("trace"));
            }));
            Mono<Value> read = f.storage.getConfig("key");
            assertEquals(0, subscriptions.get());
            StepVerifier.create(read.contextWrite(ctx -> ctx.put("trace", "caller")), 0)
                .thenRequest(1).expectNextMatches(value -> value.asString().equals("caller"))
                .expectComplete().verify(LIMIT);
            assertEquals(1, subscriptions.get());
        }
    }

    @Test public void aBatchKeepsItsRequestSnapshotAcrossCallerMutationRetryAndResubscription() {
        try (Fixture f = new Fixture()) {
            List<String> keys = new ArrayList<>(Arrays.asList("a", "b"));
            AtomicInteger attempts = new AtomicInteger();
            when(f.backend.get(anyCollection())).thenAnswer(invocation -> {
                assertEquals(new HashSet<>(Arrays.asList("a", "b")),
                    new HashSet<>((Collection<?>) invocation.getArgument(0)));
                return Flux.deferContextual(context -> {
                    assertEquals("caller", context.get("trace"));
                    return attempts.incrementAndGet() == 1
                        ? Flux.error(new IllegalStateException("transient"))
                        : Flux.just(entry("a", "value"), entry("b", null));
                });
            });
            Mono<Values> read = f.storage.getConfigs(keys).contextWrite(context -> context.put("trace", "caller"));
            keys.clear();
            keys.add("different");
            assertEquals(Collections.singletonMap("a", "value"), read.retry(1).block(LIMIT).getAllValues());
            f.storage.clearLocalCache(CacheNotify.expiresAll("lifecycle"));
            assertEquals(Collections.singletonMap("a", "value"), read.block(LIMIT).getAllValues());
            assertEquals(3, attempts.get());
            verify(f.backend, times(3)).get(anyCollection());
        }
    }

    @Test public void aBatchSelectsAnEqualButDistinctCommittedValueAndOmitsNegativeResults() {
        try (Fixture f = new Fixture()) {
            String oldValue = new String("equal");
            String newValue = new String("equal");
            Sinks.Empty<Map.Entry<String, Object>> end = Sinks.empty();
            when(f.backend.get(anyCollection())).thenReturn(Flux.concat(
                Flux.just(entry("key", oldValue), entry("negative", null)), end.asMono()));
            when(f.backend.put("key", newValue)).thenReturn(Mono.just(true));
            CompletableFuture<Values> read = f.storage.getConfigs(Arrays.asList("key", "negative", "missing")).toFuture();
            assertTrue(f.storage.setConfig("key", newValue).block(LIMIT));
            assertFalse(read.isDone());
            end.tryEmitEmpty();
            Map<String, Object> values = read.orTimeout(3, TimeUnit.SECONDS).join().getAllValues();
            assertEquals(Collections.singletonMap("key", newValue), values);
            assertSame(newValue, values.get("key"));
        }
    }

    @Test public void negativeCacheStillRunsTheExplicitFallbackWithContext() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            assertNull(f.storage.getConfig("key").block(LIMIT));
            Value value = f.storage.getConfig("key", Mono.deferContextual(ctx -> Mono.just(ctx.get("default"))))
                .contextWrite(ctx -> ctx.put("default", "fallback")).block(LIMIT);
            assertEquals("fallback", value.asString());
            assertEquals("fallback", f.storage.getConfig("key").block(LIMIT).asString());
            verify(f.backend, times(1)).get("key");
        }
    }

    @Test public void retryOfTheSameSinglePublisherStartsANewLoad() {
        try (Fixture f = new Fixture()) {
            AtomicInteger attempts = new AtomicInteger();
            when(f.backend.get("key")).thenReturn(Mono.defer(() -> attempts.incrementAndGet() == 1
                ? Mono.error(new IllegalStateException("transient")) : Mono.just("recovered")));
            assertEquals("recovered", f.storage.getConfig("key").retry(1).block(LIMIT).asString());
            assertEquals(2, attempts.get());
            assertEquals(1, f.manager.monitor.loadErrors.sum());
        }
    }

    @Test public void aFallbackUsesItsOwnSingleReadWhileABatchIsPending() {
        try (Fixture f = new Fixture()) {
            Sinks.Empty<Map.Entry<String, Object>> source = Sinks.empty();
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flux());
            when(f.backend.get("key")).thenReturn(Mono.empty());
            CompletableFuture<Values> batch = f.storage.getConfigs(Collections.singleton("key")).toFuture();
            CompletableFuture<Value> single = f.storage.getConfig("key", Mono.just("fallback")).toFuture();
            source.tryEmitEmpty();
            assertEquals("fallback", batch.orTimeout(3, TimeUnit.SECONDS).join().getString("key", ""));
            assertEquals("fallback", single.orTimeout(3, TimeUnit.SECONDS).join().asString());
            verify(f.backend, times(1)).get("key");
        }
    }

    @Test public void valueAndVersionPublishTogetherEvenWhenSharingAnOldValuePauses() throws Exception {
        try (Fixture f = new Fixture()) {
            Sinks.One<Object> source = Sinks.one();
            when(f.backend.get("productName")).thenReturn(source.asMono());
            when(f.backend.put("productName", "newer")).thenReturn(Mono.just(true));
            CompletableFuture<Value> read = f.storage.getConfig("productName").toFuture();
            LocalCacheClusterConfigStorage.Cache cache = f.entries.get("productName");
            CountDownLatch hashing = new CountDownLatch(1);
            CountDownLatch resume = new CountDownLatch(1);
            AtomicBoolean first = new AtomicBoolean(true);
            Object older = new Object() {
                @Override public int hashCode() {
                    if (first.compareAndSet(true, false)) {
                        hashing.countDown();
                        try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                        catch (InterruptedException error) { throw new AssertionError(error); }
                    }
                    return 183219;
                }
            };
            CompletableFuture<Void> old = CompletableFuture.runAsync(() -> source.tryEmitValue(older));
            try {
                assertTrue(hashing.await(2, TimeUnit.SECONDS));
                assertEquals(0, cache.version());
                assertTrue(f.storage.setConfig("productName", "newer").block(LIMIT));
                assertEquals("newer", read.get(2, TimeUnit.SECONDS).asString());
            } finally { resume.countDown(); }
            old.get(2, TimeUnit.SECONDS);
            assertEquals(1, cache.version());
            assertEquals("newer", cache.getCachedValue());
        }
    }

    @Test public void aClaimedSingleResultStillDeliversAfterANewerWriteBeforeEmission() throws Exception {
        CountDownLatch emitting = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.<Object>just("loaded").doOnCancel(() -> {
                emitting.countDown();
                try { assertTrue(resume.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException error) { throw new AssertionError(error); }
            }));
            when(f.backend.put("key", "newer")).thenReturn(Mono.just(true));
            CompletableFuture<Value> read = new CompletableFuture<>();
            Future<?> delivery = executor.submit(() -> f.storage.getConfig("key")
                .subscribe(read::complete, read::completeExceptionally));
            try {
                assertTrue(emitting.await(2, TimeUnit.SECONDS));
                assertTrue(f.storage.setConfig("key", "newer").block(LIMIT));
                assertFalse(read.isDone());
            } finally { resume.countDown(); }
            delivery.get(2, TimeUnit.SECONDS);
            assertEquals("loaded", read.get(2, TimeUnit.SECONDS).asString());
            when(f.backend.get("key")).thenReturn(Mono.just("newer"));
            assertEquals("newer", f.storage.getConfig("key").block(LIMIT).asString());
            assertEquals(0, f.manager.monitor.loadErrors.sum());
        } finally { resume.countDown(); executor.shutdownNow(); }
    }

    @Test public void aCommittedWriteSurvivesWhenTheIndependentBatchLaterCompletes() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Map.Entry<String, Object>> source = Sinks.one();
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flux());
            when(f.backend.put("key", "new")).thenReturn(Mono.just(true));
            when(f.backend.get("key")).thenReturn(Mono.just("new"));
            CompletableFuture<Values> old = f.storage.getConfigs(Collections.singleton("key")).toFuture();
            assertTrue(f.storage.setConfig("key", "new").block(LIMIT));
            assertFalse(old.isDone());
            source.tryEmitValue(entry("key", "old"));
            assertEquals("new", old.orTimeout(3, TimeUnit.SECONDS).join().getString("key", ""));
            assertEquals("new", f.storage.getConfig("key").block(LIMIT).asString());
        }
    }

    @Test public void overlappingBatchesAndSinglesHaveIndependentRequests() {
        try (Fixture f = new Fixture()) {
            List<Set<String>> requested = new ArrayList<>();
            Sinks.One<Map<String, Object>> first = Sinks.one();
            Sinks.One<Map<String, Object>> second = Sinks.one();
            when(f.backend.get(anyCollection())).thenAnswer(call -> {
                Set<String> keys = new LinkedHashSet<>(call.getArgument(0));
                requested.add(keys);
                return (requested.size() == 1 ? first : second).asMono()
                    .flatMapMany(values -> Flux.fromIterable(values.entrySet()));
            });
            when(f.backend.get("b")).thenReturn(Mono.just("B"));
            CompletableFuture<Values> ab = f.storage.getConfigs(Arrays.asList("a", "b")).toFuture();
            CompletableFuture<Values> bc = f.storage.getConfigs(Arrays.asList("b", "c")).toFuture();
            CompletableFuture<Value> b = f.storage.getConfig("b").toFuture();
            assertEquals(Arrays.asList(new LinkedHashSet<>(Arrays.asList("a", "b")),
                new LinkedHashSet<>(Arrays.asList("b", "c"))), requested);
            Map<String, Object> secondValues = new LinkedHashMap<>();
            secondValues.put("b", "B"); secondValues.put("c", "C");
            second.tryEmitValue(secondValues);
            assertTrue(bc.isDone());
            assertFalse(ab.isDone());
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("a", "A"); values.put("b", "B");
            first.tryEmitValue(values);
            assertEquals("B", b.orTimeout(3, TimeUnit.SECONDS).join().asString());
            assertEquals("A", ab.orTimeout(3, TimeUnit.SECONDS).join().getString("a", ""));
            assertEquals("C", bc.orTimeout(3, TimeUnit.SECONDS).join().getString("c", ""));
            verify(f.backend, times(1)).get("b");
        }
    }

    @Test public void tenIdenticalConcurrentBatchesEachMakeOneBatchRequest() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Map.Entry<String, Object>> source = Sinks.one();
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flux());
            List<CompletableFuture<Values>> readers = new ArrayList<>();
            for (int i = 0; i < 10; i++) readers.add(f.storage.getConfigs(Collections.singleton("key")).toFuture());
            verify(f.backend, times(10)).get(anyCollection());
            source.tryEmitValue(entry("key", "value"));
            for (CompletableFuture<Values> read : readers)
                assertEquals("value", read.orTimeout(3, TimeUnit.SECONDS).join().getString("key", ""));
        }
    }

    @Test public void cancelledReadersLeaveTheSharedFillForLaterReaders() {
        try (Fixture f = new Fixture()) {
            AtomicInteger cancellations = new AtomicInteger();
            Sinks.One<Object> source = Sinks.one();
            when(f.backend.get("key")).thenReturn(source.asMono().doOnCancel(cancellations::incrementAndGet));
            Disposable first = f.storage.getConfig("key").subscribe();
            Disposable second = f.storage.getConfig("key").subscribe();
            first.dispose(); second.dispose(); assertEquals(0, cancellations.get());
            CompletableFuture<Value> third = f.storage.getConfig("key").toFuture();
            source.tryEmitValue("filled");
            assertEquals("filled", third.orTimeout(3, TimeUnit.SECONDS).join().asString());
            verify(f.backend, times(1)).get("key");
            assertEquals(0, f.manager.monitor.loadErrors.sum());
        }
    }

    @Test public void cancellingABatchDoesNotCancelAnIndependentSingle() {
        try (Fixture f = new Fixture()) {
            AtomicInteger cancellations = new AtomicInteger();
            Sinks.One<Map.Entry<String, Object>> source = Sinks.one();
            when(f.backend.get(anyCollection())).thenReturn(source.asMono().flux().doOnCancel(cancellations::incrementAndGet));
            Sinks.One<Object> singleSource = Sinks.one();
            when(f.backend.get("a")).thenReturn(singleSource.asMono());
            Disposable batch = f.storage.getConfigs(Arrays.asList("a", "b")).subscribe();
            CompletableFuture<Value> single = f.storage.getConfig("a").toFuture();
            batch.dispose();
            assertEquals(1, cancellations.get());
            assertFalse(single.isDone());
            singleSource.tryEmitValue("A");
            assertEquals("A", single.orTimeout(3, TimeUnit.SECONDS).join().asString());
        }
    }

    @Test public void anUnderlyingSingleTimeoutAllowsRecovery() {
        try (Fixture f = new Fixture()) {
            AtomicInteger cancellations = new AtomicInteger();
            when(f.backend.get("key")).thenReturn(Mono.defer(() -> Mono.<Object>never()
                .doOnCancel(cancellations::incrementAndGet).timeout(Duration.ofSeconds(30))));
            StepVerifier.withVirtualTime(() -> f.storage.getConfig("key"))
                .expectSubscription().thenAwait(Duration.ofSeconds(30)).expectError(TimeoutException.class).verify(LIMIT);
            assertEquals(1, cancellations.get());
            assertEquals(1, f.manager.monitor.loadErrors.sum());
            when(f.backend.get("key")).thenReturn(Mono.just("recovered"));
            assertEquals("recovered", f.storage.getConfig("key").block(LIMIT).asString());
        }
    }

    @Test public void partialBatchHitsRenewPositiveAndNegativeIdleExpiry() {
        try (Fixture f = new Fixture()) {
            Map<String, LocalCacheClusterConfigStorage.Cache> entries = new ConcurrentHashMap<>();
            LocalCacheClusterConfigStorage partial = new LocalCacheClusterConfigStorage(
                "partial-idle", f.manager, f.backend, 60_000, null, entries);
            f.manager.cache.put("partial-idle", partial);
            when(f.backend.get("hot")).thenReturn(Mono.just("H"));
            when(f.backend.get("negative")).thenReturn(Mono.empty());
            partial.getConfig("hot").block(LIMIT);
            partial.getConfig("negative").block(LIMIT);
            // getCachedValues is deliberately bypassed by putting the miss first.
            long before = System.nanoTime() - TimeUnit.SECONDS.toNanos(30);
            for (String key : Arrays.asList("hot", "negative")) {
                entries.get(key).lastAccessNanos = before;
            }
            when(f.backend.get(anyCollection())).thenReturn(Flux.just(entry("miss", "M")));
            Values values = partial.getConfigs(Arrays.asList("miss", "hot", "negative")).block(LIMIT);
            assertEquals("H", values.getString("hot", ""));
            assertFalse(values.getAllValues().containsKey("negative"));
            assertTrue(entries.get("hot").lastAccessNanos - before > 0);
            assertTrue(entries.get("negative").lastAccessNanos - before > 0);
            verify(f.backend).get(Collections.singleton("miss"));
        }
    }

    @Test public void idleExpiryUsesElapsedNanosAndTouchRenewsIt() {
        try (Fixture f = new Fixture()) {
            LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
                "idle-clock", f.manager, f.backend, 60_000, null);
            LocalCacheClusterConfigStorage.Cache cache = storage.new Cache("key");
            cache.lastAccessNanos = System.nanoTime() - TimeUnit.SECONDS.toNanos(30);
            assertFalse(cache.isExpired());
            cache.lastAccessNanos = System.nanoTime() - TimeUnit.SECONDS.toNanos(61);
            assertTrue(cache.isExpired());
            long before = System.nanoTime();
            cache.touch();
            assertTrue(cache.lastAccessNanos - before >= 0);
            assertFalse(cache.isExpired());
        }
    }

    @Test public void nonPositiveIdleTtlDoesNotExpireOrTouchEntries() {
        try (Fixture f = new Fixture()) {
            for (long ttl : new long[]{0, -1, Long.MIN_VALUE}) {
                LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
                    "idle-disabled", f.manager, f.backend, ttl, null);
                LocalCacheClusterConfigStorage.Cache cache = storage.new Cache("key");
                long old = System.nanoTime() - TimeUnit.DAYS.toNanos(365);
                cache.lastAccessNanos = old;
                assertFalse(cache.isExpired());
                cache.touch();
                assertEquals(old, cache.lastAccessNanos);
            }
        }
    }

    @Test public void veryLargeIdleTtlStillRenewsWithoutConversionOverflow() {
        try (Fixture f = new Fixture()) {
            LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
                "idle-large", f.manager, f.backend, Long.MAX_VALUE, null);
            LocalCacheClusterConfigStorage.Cache cache = storage.new Cache("key");
            long old = System.nanoTime() - TimeUnit.SECONDS.toNanos(61);
            cache.lastAccessNanos = old;
            assertFalse(cache.isExpired());
            cache.touch();
            assertTrue(cache.lastAccessNanos - old >= TimeUnit.SECONDS.toNanos(61));
            assertFalse(cache.isExpired());
        }
    }

    @Test public void anUnderlyingPartialBatchTimeoutReleasesEveryOwnedKey() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get(anyCollection())).thenReturn(Flux.defer(() ->
                Flux.concat(Flux.just(entry("a", "abandoned")), Flux.never()).timeout(Duration.ofSeconds(30))));
            StepVerifier.withVirtualTime(() -> f.storage.getConfigs(Arrays.asList("a", "b")))
                .expectSubscription().thenAwait(Duration.ofSeconds(30)).expectError(TimeoutException.class).verify(LIMIT);
            when(f.backend.get(anyCollection())).thenReturn(Flux.just(entry("b", "recovered")));
            Values result = f.storage.getConfigs(Arrays.asList("a", "b")).block(LIMIT);
            assertEquals(Collections.singletonMap("b", "recovered"), result.getAllValues());
        }
    }

    @Test public void aCallerControlledFallbackTimeoutReleasesTheKey() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.empty());
            AtomicInteger cancellations = new AtomicInteger();
            StepVerifier.withVirtualTime(() -> f.storage.getConfig("key",
                    Mono.<Object>never().doOnCancel(cancellations::incrementAndGet).timeout(Duration.ofSeconds(30))))
                .expectSubscription().thenAwait(Duration.ofSeconds(30)).expectError(TimeoutException.class).verify(LIMIT);
            assertEquals(1, cancellations.get());
            assertNull(f.storage.getConfig("key").block(LIMIT));
        }
    }

    @Test public void anUnderlyingTimeoutAndReentrantRetryCannotReuseAPendingSlot() {
        try (Fixture f = new Fixture()) {
            AtomicInteger attempts = new AtomicInteger();
            when(f.backend.get("key")).thenReturn(Mono.defer(() -> attempts.incrementAndGet() == 1
                ? Mono.never().timeout(Duration.ofSeconds(30)) : Mono.just("recovered")));
            StepVerifier.withVirtualTime(() -> f.storage.getConfig("key").retry(1))
                .expectSubscription().thenAwait(Duration.ofSeconds(30))
                .expectNextMatches(value -> value.asString().equals("recovered"))
                .expectComplete().verify(LIMIT);
            assertEquals(2, attempts.get());
        }
    }

    @Test public void synchronousSingleCreationFailureClearsItsSlotBeforeRetry() {
        try (Fixture f = new Fixture()) {
            AtomicInteger attempts = new AtomicInteger();
            when(f.backend.get("key")).thenAnswer(ignored -> {
                if (attempts.incrementAndGet() == 1) throw new IllegalStateException("creation");
                return Mono.just("recovered");
            });
            Mono<Value> read = f.storage.getConfig("key");
            assertEquals("recovered", read.retry(1).block(LIMIT).asString());
            assertEquals("recovered", read.block(LIMIT).asString());
            assertEquals(2, attempts.get());
            assertEquals(1, f.manager.monitor.loadErrors.sum());
        }
    }

    @Test public void synchronousBackendCreationFailureReleasesAllKeys() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get(anyCollection())).thenThrow(new IllegalStateException("creation"));
            StepVerifier.create(f.storage.getConfigs(Arrays.asList("a", "b")))
                .expectErrorMessage("creation").verify(LIMIT);
            doReturn(Flux.just(entry("a", "A"), entry("b", "B"))).when(f.backend).get(anyCollection());
            assertEquals(2, f.storage.getConfigs(Arrays.asList("a", "b")).block(LIMIT).size());
        }
    }

    @Test public void detachedSharedFillCanFinishAfterManagerShutdown() {
        try (Fixture f = new Fixture()) {
            Sinks.One<Object> source = Sinks.one();
            when(f.backend.get("key")).thenReturn(source.asMono());
            CompletableFuture<Value> read = f.storage.getConfig("key").toFuture();
            f.storage.clearLocalCache(CacheNotify.clear("lifecycle")); f.manager.dispose();
            assertFalse(read.isDone()); source.tryEmitValue("finished");
            assertEquals("finished", read.orTimeout(3, TimeUnit.SECONDS).join().asString());
            assertTrue(f.manager.cache.isEmpty());
        }
    }

    @Test public void heldHandleRejoinsAfterClearAndAfterEmptyCleanup() {
        try (Fixture f = new Fixture()) {
            AtomicReference<String> value = new AtomicReference<>("old");
            when(f.backend.get("key")).thenAnswer(ignored -> Mono.just(value.get()));
            when(f.backend.clear()).thenReturn(Mono.empty());
            f.storage.getConfig("key").block(LIMIT);
            f.storage.clear().block(LIMIT);
            f.manager.cleanup();
            value.set("after-clear");
            assertEquals("after-clear", f.storage.getConfig("key").block(LIMIT).asString());
            value.set("remote-change");
            f.manager.handleNotify(CacheNotify.expires("lifecycle", Collections.singleton("key")));
            assertEquals("remote-change", f.storage.getConfig("key").block(LIMIT).asString());
        }
    }

    @Test public void emptyGetAndRemoveStillInvalidatesLocalNegativeOrStaleValues() {
        try (Fixture f = new Fixture()) {
            when(f.backend.get("key")).thenReturn(Mono.just("old"), Mono.empty());
            when(f.backend.getAndRemove("key")).thenReturn(Mono.empty());
            f.storage.getConfig("key").block(LIMIT);
            assertNull(f.storage.getAndRemove("key").block(LIMIT));
            assertNull(f.storage.getConfig("key").block(LIMIT));
        }
    }

    @Test public void orphanedHandleWritesAndRefreshesTheCanonicalStorage() {
        try (Fixture f = new Fixture()) {
            f.storage.clearLocalCache(CacheNotify.clear("lifecycle"));
            LocalCacheClusterConfigStorage replacement = new LocalCacheClusterConfigStorage("lifecycle", f.manager,
                f.backend, -1, null);
            f.manager.cache.put("lifecycle", replacement);
            Sinks.One<Object> source = Sinks.one();
            when(f.backend.get("key")).thenReturn(source.asMono());
            when(f.backend.put("key", "new")).thenReturn(Mono.just(true));
            CompletableFuture<Value> pending = replacement.getConfig("key").toFuture();
            f.storage.setConfig("key", "new").block(LIMIT);
            assertEquals("new", pending.join().asString());
            when(f.backend.get("key")).thenReturn(Mono.just("after-write"));
            assertEquals("after-write", replacement.getConfig("key").block(LIMIT).asString());
            when(f.backend.get("key")).thenReturn(Mono.just("after-refresh"));
            f.storage.refresh().block(LIMIT);
            assertEquals("after-refresh", replacement.getConfig("key").block(LIMIT).asString());
        }
    }
}
