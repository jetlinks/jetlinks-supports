package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceProductOperator;
import org.junit.Test;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MonoValidatedDeviceOperatorTest {

    @Test
    public void shouldValidateOnceUntilInvalidated() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator device = mock(DeviceOperator.class);
        DeviceProductOperator product = mock(DeviceProductOperator.class);
        AtomicInteger subscriptions = new AtomicInteger();
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            device,
            Mono.defer(() -> {
                subscriptions.incrementAndGet();
                return Mono.just(product);
            }),
            cache
        );

        assertSame(device, source.block());
        assertSame(device, source.block());
        assertEquals(1, subscriptions.get());
        assertTrue(source.isValidated());

        source.invalidate();
        cache.invalidate("test");

        assertSame(device, source.block());
        assertEquals(2, subscriptions.get());
        assertTrue(source.isValidated());
    }

    @Test
    public void shouldRevalidateWhenInvalidatedBeforeDemand() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator device = mock(DeviceOperator.class);
        DeviceProductOperator product = mock(DeviceProductOperator.class);
        AtomicBoolean exists = new AtomicBoolean(true);
        AtomicInteger subscriptions = new AtomicInteger();
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            device,
            Mono.defer(() -> {
                subscriptions.incrementAndGet();
                return exists.get() ? Mono.just(product) : Mono.empty();
            }),
            cache
        );

        assertSame(device, source.block());
        StepVerifier.create(source, 0)
                    .then(() -> {
                        exists.set(false);
                        source.invalidate();
                        cache.invalidate("test");
                    })
                    .thenRequest(1)
                    .verifyComplete();

        assertEquals(2, subscriptions.get());
        assertFalse(source.isValidated());
    }

    @Test
    public void shouldPropagateContextAndCancellationAfterInvalidationBeforeDemand() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        AtomicReference<Mono<DeviceProductOperator>> validation = new AtomicReference<>(
            Mono.just(mock(DeviceProductOperator.class))
        );
        AtomicReference<String> contextValue = new AtomicReference<>();
        AtomicInteger cancellations = new AtomicInteger();
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.deferContextual(context -> {
                contextValue.set(context.getOrDefault("trace", ""));
                return validation.get();
            }),
            cache
        );

        assertNotNull(source.block());
        validation.set(Mono.<DeviceProductOperator>never().doOnCancel(cancellations::incrementAndGet));
        StepVerifier.create(source.contextWrite(context -> context.put("trace", "new")), 0)
                    .then(() -> {
                        source.invalidate();
                        cache.invalidate("test");
                    })
                    .thenRequest(1)
                    .thenCancel()
                    .verify();

        assertEquals("new", contextValue.get());
        assertEquals(1, cancellations.get());
    }

    @Test
    public void shouldPropagateValidationErrorAfterInvalidationBeforeDemand() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        AtomicReference<Mono<DeviceProductOperator>> validation = new AtomicReference<>(
            Mono.just(mock(DeviceProductOperator.class))
        );
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test", mock(DeviceOperator.class), Mono.defer(validation::get), cache
        );

        assertNotNull(source.block());
        validation.set(Mono.error(new IllegalStateException("invalid")));
        StepVerifier.create(source, 0)
                    .then(() -> {
                        source.invalidate();
                        cache.invalidate("test");
                    })
                    .thenRequest(1)
                    .expectErrorMessage("invalid")
                    .verify();
    }

    @Test
    public void shouldRemoveEmptyDeviceFromCache() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.empty(),
            cache
        );

        StepVerifier.create(source)
                    .verifyComplete();

        assertNull(cache.getIfPresent("test"));
    }

    @Test
    public void shouldDelegateConcurrentLookupToCachedPublisher() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator first = mock(DeviceOperator.class);
        DeviceOperator second = mock(DeviceOperator.class);
        MonoValidatedDeviceOperator cached = new MonoValidatedDeviceOperator(
            "test",
            first,
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        MonoValidatedDeviceOperator raced = new MonoValidatedDeviceOperator(
            "test",
            second,
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );

        assertSame(first, cached.block());
        assertSame(first, raced.block());
    }

    @Test
    public void shouldNotRecursivelyDelegateWhenCachedOperatorChanges() {
        Cache<String, Mono<DeviceOperator>> cache = mock(Cache.class);
        DeviceOperator firstDevice = mock(DeviceOperator.class);
        DeviceOperator secondDevice = mock(DeviceOperator.class);
        MonoValidatedDeviceOperator first = new MonoValidatedDeviceOperator(
            "test",
            firstDevice,
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        MonoValidatedDeviceOperator second = new MonoValidatedDeviceOperator(
            "test",
            secondDevice,
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        AtomicInteger lookups = new AtomicInteger();
        when(cache.getIfPresent("test")).thenAnswer(ignored ->
            (lookups.getAndIncrement() & 1) == 0 ? second : first);

        StepVerifier.create(first)
                    .expectNext(secondDevice)
                    .verifyComplete();

        assertEquals(2, lookups.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotRecursivelyDelegateWhenPutIfAbsentLosesRace() {
        Cache<String, Mono<DeviceOperator>> cache = mock(Cache.class);
        ConcurrentMap<String, Mono<DeviceOperator>> entries = mock(ConcurrentMap.class);
        when(cache.asMap()).thenReturn(entries);
        DeviceOperator cachedDevice = mock(DeviceOperator.class);
        MonoValidatedDeviceOperator cached = new MonoValidatedDeviceOperator(
            "test",
            cachedDevice,
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        AtomicInteger lookups = new AtomicInteger();
        when(cache.getIfPresent("test")).thenAnswer(ignored ->
            lookups.getAndIncrement() == 0 ? null : source);
        when(entries.putIfAbsent("test", source)).thenReturn(cached);

        StepVerifier.create(source)
                    .expectNext(cachedDevice)
                    .verifyComplete();

        assertEquals(2, lookups.get());
    }

    @Test
    public void shouldDelegateThroughAssemblyWrapperWithoutRecursion() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator cachedDevice = mock(DeviceOperator.class);
        AtomicReference<String> contextValue = new AtomicReference<>();
        AtomicInteger contextSize = new AtomicInteger();
        MonoValidatedDeviceOperator cached = new MonoValidatedDeviceOperator(
            "test",
            cachedDevice,
            Mono.deferContextual(context -> {
                contextValue.set(context.get("trace"));
                contextSize.set(context.size());
                return Mono.just(mock(DeviceProductOperator.class));
            }),
            cache
        );
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );

        Hooks.onOperatorDebug();
        try {
            cache.put("test", cached.hide());
            StepVerifier.create(source.contextWrite(Context.of("trace", "assembly")))
                        .expectNext(cachedDevice)
                        .verifyComplete();
        } finally {
            Hooks.resetOnOperatorDebug();
        }
        assertEquals("assembly", contextValue.get());
        assertEquals(1, contextSize.get());
    }

    @Test
    public void shouldScopeDelegationToCacheInstance() {
        Cache<String, Mono<DeviceOperator>> firstCache = CacheBuilder.newBuilder().build();
        Cache<String, Mono<DeviceOperator>> secondCache = CacheBuilder.newBuilder().build();
        DeviceOperator expected = mock(DeviceOperator.class);
        MonoValidatedDeviceOperator nested = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            secondCache
        );
        MonoValidatedDeviceOperator nestedCached = new MonoValidatedDeviceOperator(
            "test",
            expected,
            Mono.just(mock(DeviceProductOperator.class)),
            secondCache
        );
        secondCache.put("test", nestedCached);

        MonoValidatedDeviceOperator cached = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            firstCache
        );
        firstCache.put("test", cached.then(nested));
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            firstCache
        );

        StepVerifier.create(source)
                    .expectNext(expected)
                    .verifyComplete();
    }

    @Test
    public void shouldRevalidateDelegatedOperatorWhenInvalidatedBeforeDemand() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator cachedDevice = mock(DeviceOperator.class);
        AtomicReference<Mono<DeviceProductOperator>> validation = new AtomicReference<>(
            Mono.just(mock(DeviceProductOperator.class))
        );
        AtomicInteger subscriptions = new AtomicInteger();
        MonoValidatedDeviceOperator cached = new MonoValidatedDeviceOperator(
            "test",
            cachedDevice,
            Mono.defer(() -> {
                subscriptions.incrementAndGet();
                return validation.get();
            }),
            cache
        );
        cache.put("test", cached);
        assertSame(cachedDevice, cached.block());

        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        StepVerifier.create(source, 0)
                    .then(() -> {
                        validation.set(Mono.empty());
                        cached.invalidate();
                    })
                    .thenRequest(1)
                    .verifyComplete();

        assertEquals(2, subscriptions.get());
        assertNull(cache.getIfPresent("test"));
    }

    @Test
    public void shouldPropagateContextAndErrorFromCachedOperator() {
        Cache<String, Mono<DeviceOperator>> cache = mock(Cache.class);
        AtomicReference<String> contextValue = new AtomicReference<>();
        MonoValidatedDeviceOperator cached = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.deferContextual(context -> {
                contextValue.set(context.get("trace"));
                return Mono.error(new IllegalStateException("invalid"));
            }),
            cache
        );
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        when(cache.getIfPresent("test")).thenReturn(cached);

        StepVerifier.create(source.contextWrite(Context.of("trace", "cached")))
                    .expectErrorMessage("invalid")
                    .verify();

        assertEquals("cached", contextValue.get());
    }

    @Test
    public void shouldCancelCachedOperatorValidation() {
        Cache<String, Mono<DeviceOperator>> cache = mock(Cache.class);
        AtomicBoolean cancelled = new AtomicBoolean();
        MonoValidatedDeviceOperator cached = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.never().doOnCancel(() -> cancelled.set(true)),
            cache
        );
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        when(cache.getIfPresent("test")).thenReturn(cached);

        StepVerifier.create(source)
                    .thenCancel()
                    .verify();

        assertTrue(cancelled.get());
    }

    @Test
    public void shouldDelegateToOrdinaryCachedPublisher() {
        Cache<String, Mono<DeviceOperator>> cache = mock(Cache.class);
        DeviceOperator cachedDevice = mock(DeviceOperator.class);
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            mock(DeviceOperator.class),
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        when(cache.getIfPresent("test")).thenReturn(Mono.just(cachedDevice));

        StepVerifier.create(source)
                    .expectNext(cachedDevice)
                    .verifyComplete();
    }

    @Test
    public void shouldNotCacheValidationCompletedAfterInvalidation() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator device = mock(DeviceOperator.class);
        DeviceProductOperator product = mock(DeviceProductOperator.class);
        Sinks.One<DeviceProductOperator> validation = Sinks.one();
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            device,
            validation.asMono(),
            cache
        );

        StepVerifier.create(source)
                    .then(() -> {
                        source.invalidate();
                        cache.invalidate("test");
                        validation.tryEmitValue(product);
                    })
                    .expectNext(device)
                    .verifyComplete();

        assertTrue(!source.isValidated());
        assertSame(device, source.block());
        assertTrue(source.isValidated());
    }

    @Test
    public void shouldHonorDemandOnValidatedFastPath() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator device = mock(DeviceOperator.class);
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            device,
            Mono.just(mock(DeviceProductOperator.class)),
            cache
        );
        assertSame(device, source.block());

        StepVerifier.create(source, 0)
                    .thenRequest(1)
                    .expectNext(device)
                    .verifyComplete();
    }

    @Test
    public void shouldInvalidateDuringValidatedFastPathEmissionAndRevalidateNextRequest() throws InterruptedException {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator device = mock(DeviceOperator.class);
        AtomicInteger validations = new AtomicInteger();
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            device,
            Mono.defer(() -> {
                validations.incrementAndGet();
                return Mono.just(mock(DeviceProductOperator.class));
            }),
            cache
        );
        assertSame(device, source.block());

        CountDownLatch emissionStarted = new CountDownLatch(1);
        CountDownLatch releaseEmission = new CountDownLatch(1);
        CountDownLatch invalidated = new CountDownLatch(1);
        Thread subscriber = new Thread(() -> source.subscribe(ignore -> {
            emissionStarted.countDown();
            try {
                releaseEmission.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        }));
        subscriber.start();
        assertTrue(emissionStarted.await(5, TimeUnit.SECONDS));

        Thread invalidator = new Thread(() -> {
            source.invalidate();
            invalidated.countDown();
        });
        try {
            invalidator.start();
            assertTrue(invalidated.await(5, TimeUnit.SECONDS));
            assertSame(device, source.block());
            assertEquals(2, validations.get());
        } finally {
            releaseEmission.countDown();
            subscriber.join(5000);
            invalidator.join(5000);
        }
        assertFalse(subscriber.isAlive());
        assertFalse(invalidator.isAlive());
    }

    @Test
    public void shouldPropagateContextAndCancellation() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator device = mock(DeviceOperator.class);
        AtomicReference<String> contextValue = new AtomicReference<>();
        MonoValidatedDeviceOperator contextual = new MonoValidatedDeviceOperator(
            "context",
            device,
            Mono.deferContextual(context -> {
                contextValue.set(context.get("key"));
                return Mono.just(mock(DeviceProductOperator.class));
            }),
            cache
        );

        StepVerifier.create(contextual.contextWrite(Context.of("key", "value")))
                    .expectNext(device)
                    .verifyComplete();
        assertEquals("value", contextValue.get());

        AtomicBoolean cancelled = new AtomicBoolean();
        MonoValidatedDeviceOperator cancellable = new MonoValidatedDeviceOperator(
            "cancel",
            device,
            Mono.never().doOnCancel(() -> cancelled.set(true)),
            cache
        );
        StepVerifier.create(cancellable)
                    .thenCancel()
                    .verify();
        assertTrue(cancelled.get());
    }

    @Test
    public void shouldRetryAfterValidationError() {
        Cache<String, Mono<DeviceOperator>> cache = CacheBuilder.newBuilder().build();
        DeviceOperator device = mock(DeviceOperator.class);
        DeviceProductOperator product = mock(DeviceProductOperator.class);
        AtomicInteger attempts = new AtomicInteger();
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "test",
            device,
            Mono.defer(() -> attempts.incrementAndGet() == 1
                ? Mono.error(new IllegalStateException("failed"))
                : Mono.just(product)),
            cache
        );

        StepVerifier.create(source)
                    .expectErrorMessage("failed")
                    .verify();
        assertFalse(source.isValidated());

        assertSame(device, source.block());
        assertSame(device, source.block());
        assertEquals(2, attempts.get());
    }
}
