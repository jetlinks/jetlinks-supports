package org.jetlinks.supports.rpc;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import io.netty.buffer.*;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.*;
import static org.jetlinks.supports.rpc.RpcSpiTestSupport.*;

public class RpcResponseMonoTest {
    static final Duration TIMEOUT = Duration.ofSeconds(10);
    static Payload frame() { return ByteBufPayload.create(Unpooled.directBuffer(8).writeLong(123), Unpooled.EMPTY_BUFFER); }
    static ByteBuf bytes(Payload frame) { return frame.data().retainedSlice(); }

    static <T> Mono<T> response(Mono<Payload> source, java.util.function.Function<Payload, T> decoder,
                                 Duration timeout, reactor.core.scheduler.Scheduler scheduler) {
        return RpcCallMono.create(new RpcCallMono.Plan<Disposable, T>(timeout, scheduler) {
            Disposable acquire(reactor.util.context.Context context, Object request) { return Disposables.disposed(); }
            Mono<?> invoke(Disposable resource, Object request) { return source; }
            T decode(Object input) {
                Payload frame = (Payload) input;
                try { return decoder.apply(frame); } finally { frame.release(); }
            }
        }, null);
    }

    static class ManualSource extends Mono<Payload> {
        CoreSubscriber<? super Payload> actual;
        final AtomicBoolean cancelled = new AtomicBoolean();
        final AtomicLong demand = new AtomicLong();
        public void subscribe(CoreSubscriber<? super Payload> subscriber) {
            actual = subscriber;
            subscriber.onSubscribe(new Subscription() {
                public void request(long n) { demand.addAndGet(n); }
                public void cancel() { cancelled.set(true); }
            });
        }
        void emit(Payload frame) { actual.onNext(frame); }
    }

    @Test public void honorsDemandContextAndTransfersOnlyTheDecodedReference() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create();
        ManualSource source = new ManualSource(); Payload frame = frame();
        try {
            StepVerifier.create(response(source, RpcResponseMonoTest::bytes, TIMEOUT, clock)
                .contextWrite(context -> context.put("rpc-test", "value")), 0)
                .then(() -> { assertEquals(0, source.demand.get()); assertEquals("value", source.actual.currentContext().get("rpc-test")); })
                .thenRequest(1).then(() -> source.emit(frame))
                .assertNext(value -> { try { assertEquals(123, value.readLong()); } finally { value.release(); } })
                .verifyComplete();
            assertEquals(0, frame.refCnt()); clock.advanceTimeBy(TIMEOUT.multipliedBy(2));
        } finally { clock.dispose(); }
    }

    @Test public void timeoutReleasesLateFramesWithoutDecodingThem() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create();
        ManualSource source = new ManualSource(); Payload frame = frame(); AtomicInteger decoded = new AtomicInteger();
        try {
            StepVerifier.create(response(source, value -> { decoded.incrementAndGet(); return bytes(value); }, TIMEOUT, clock), 0)
                .then(() -> clock.advanceTimeBy(TIMEOUT)).expectError(TimeoutException.class).verify(WAIT);
            source.emit(frame);
            assertTrue(source.cancelled.get()); assertEquals(0, decoded.get()); assertEquals(0, frame.refCnt());
        } finally { clock.dispose(); }
    }

    @Test public void timeoutDuringDecodeDoesNotFreeTheBorrowAndReleasesTheLosingResult() throws Exception {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create();
        ManualSource source = new ManualSource(); Payload frame = frame();
        CountDownLatch entered = new CountDownLatch(1), resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor(); AtomicReference<ByteBuf> result = new AtomicReference<>();
        AtomicReference<Future<?>> emission = new AtomicReference<>();
        try {
            Mono<ByteBuf> response = response(source, value -> {
                result.set(bytes(value)); entered.countDown();
                try { if (!resume.await(5, TimeUnit.SECONDS)) throw new AssertionError("decoder was not resumed"); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
                assertEquals(123, value.data().getLong(0));
                return result.get();
            }, TIMEOUT, clock);
            StepVerifier.create(response).then(() -> emission.set(executor.submit(() -> source.emit(frame))))
                .then(() -> { try { assertTrue(entered.await(5, TimeUnit.SECONDS)); } catch (InterruptedException e) { throw new AssertionError(e); } })
                .then(() -> clock.advanceTimeBy(TIMEOUT)).expectError(TimeoutException.class).verify(WAIT);
            assertEquals(1, frame.refCnt()); assertEquals(123, frame.data().getLong(0));
            resume.countDown(); emission.get().get(5, TimeUnit.SECONDS);
            assertEquals(0, frame.refCnt()); assertEquals(0, result.get().refCnt());
        } finally { resume.countDown(); executor.shutdownNow(); clock.dispose(); }
    }

    @Test public void cancellationDuringDecodeReleasesTheResultAndSuppressesSignals() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create(); Payload frame = frame();
        AtomicInteger signals = new AtomicInteger(); AtomicReference<ByteBuf> result = new AtomicReference<>();
        BaseSubscriber<ByteBuf> actual = new BaseSubscriber<>() {
            protected void hookOnSubscribe(Subscription subscription) { request(1); }
            protected void hookOnNext(ByteBuf value) { signals.incrementAndGet(); value.release(); }
            protected void hookOnError(Throwable error) { signals.incrementAndGet(); }
            protected void hookOnComplete() { signals.incrementAndGet(); }
        };
        try {
            response(Mono.just(frame), value -> {
                result.set(bytes(value)); actual.dispose(); assertEquals(123, value.data().getLong(0)); return result.get();
            }, TIMEOUT, clock).subscribe(actual);
            clock.advanceTimeBy(TIMEOUT.multipliedBy(2));
            assertEquals(0, signals.get()); assertEquals(0, frame.refCnt()); assertEquals(0, result.get().refCnt());
        } finally { clock.dispose(); }
    }

    @Test public void immediateCancelDoesNotSubscribeAndSubscriptionsHaveIndependentTimers() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create(); AtomicInteger subscribed = new AtomicInteger();
        Mono<ByteBuf> response = response(Mono.defer(() -> { subscribed.incrementAndGet(); return Mono.never(); }), RpcResponseMonoTest::bytes, TIMEOUT, clock);
        AtomicInteger errors = new AtomicInteger();
        try {
            StepVerifier.create(response, 0).thenCancel().verify(); assertEquals(0, subscribed.get());
            response.subscribe(ByteBuf::release, error -> { assertTrue(error instanceof TimeoutException); errors.incrementAndGet(); });
            clock.advanceTimeBy(Duration.ofSeconds(6));
            response.subscribe(ByteBuf::release, error -> { assertTrue(error instanceof TimeoutException); errors.incrementAndGet(); });
            clock.advanceTimeBy(Duration.ofSeconds(4)); assertEquals(1, errors.get());
            clock.advanceTimeBy(Duration.ofSeconds(6)); assertEquals(2, errors.get());
        } finally { clock.dispose(); }
    }

    @Test public void preservesEmptyAndErrorsAndReleasesFramesWhenDecoderFails() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create(); Payload frame = frame(); RuntimeException error = new IllegalStateException("decode");
        try {
            StepVerifier.create(response(Mono.empty(), RpcResponseMonoTest::bytes, TIMEOUT, clock)).verifyComplete();
            StepVerifier.create(response(Mono.error(error), RpcResponseMonoTest::bytes, TIMEOUT, clock)).expectErrorMatches(value -> value == error).verify();
            StepVerifier.create(response(Mono.just(frame), value -> { throw error; }, TIMEOUT, clock)).expectErrorMatches(value -> value == error).verify();
            assertEquals(0, frame.refCnt());
        } finally { clock.dispose(); }
    }

    @Test public void timerRejectionCancelsTheSourceAndFailsTheCall() {
        var scheduler = Schedulers.newSingle("rejected-rpc-timer"); scheduler.dispose(); ManualSource source = new ManualSource();
        StepVerifier.create(response(source, RpcResponseMonoTest::bytes, TIMEOUT, scheduler)).expectError(RejectedExecutionException.class).verify(WAIT);
        assertTrue(source.cancelled.get());
    }

    @Test @SuppressWarnings("unchecked") public void actualRpcTimeoutAfterDecodeReturnsEveryReference() {
        try (DefaultRpcManagerTest.Fixture fixture = new DefaultRpcManagerTest.Fixture()) {
            VirtualTimeScheduler clock = VirtualTimeScheduler.create(); AtomicReference<ByteBuf> response = new AtomicReference<>();
            ByteBuf owner = Unpooled.directBuffer(8).writeLong(123);
            Hooks.onEachOperator("rpc-response-race", publisher -> {
                if (!(publisher instanceof RpcCallMono<?, ?> original)) return publisher;
                RpcCallMono.Plan delegate = original.plan;
                var plan = new RpcCallMono.Plan<Disposable, Object>(TIMEOUT, clock) {
                    Disposable acquire(reactor.util.context.Context context, Object request) { return delegate.acquire(context, request); }
                    Mono<?> invoke(Disposable resource, Object request) { return delegate.invoke(resource, request); }
                    Object decode(Object input) {
                        Object value = delegate.decode(input);
                        if (value instanceof ByteBuf bytes) {
                            response.set(bytes); clock.advanceTimeBy(TIMEOUT.plusSeconds(1)); assertEquals(123, bytes.getLong(0));
                        }
                        return value;
                    }
                };
                return new RpcCallMono<>(plan, original.request);
            });
            try {
                StepVerifier.create(fixture.echo.bytes(owner)).expectError(TimeoutException.class).verify(WAIT);
                await(() -> response.get() != null && response.get().refCnt() == 0 && owner.refCnt() == 1 && fixture.first.activeCalls() == 0);
            } finally { Hooks.resetOnEachOperator("rpc-response-race"); owner.release(); clock.dispose(); }
        }
    }
}
