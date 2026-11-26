package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.jetlinks.core.rpc.transport.RpcFrame;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import reactor.util.context.Context;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.Assert.*;
import static org.jetlinks.supports.rpc.RpcSpiTestSupport.*;

public class RpcServerMonoTest {
    static RpcFrame request() { return new RpcFrame(Unpooled.directBuffer(8).writeLong(123), Unpooled.EMPTY_BUFFER); }
    abstract static class Plan extends RpcServerMono.Plan {
        final AtomicInteger active = new AtomicInteger();
        AtomicInteger acquire() { active.incrementAndGet(); return active; }
        RpcFrame encode(Object value) { return new RpcFrame((ByteBuf) value, Unpooled.EMPTY_BUFFER); }
    }
    static void consume(RpcFrame frame) {
        try { assertEquals(123, frame.data().readLong()); } finally { frame.release(); }
    }

    @Test public void successPreservesDemandAndRestoredContextAndReleasesOneRequestLease() {
        RpcFrame request = request(); AtomicLong demand = new AtomicLong();
        Plan plan = new Plan() {
            Mono<?> invoke(RpcFrame frame, RpcServerMono.CallSubscriber actual) {
                assertEquals("tenant", actual.currentContext().get("tenant"));
                actual.context = actual.currentContext().put("rpc-user", "user");
                return Mono.deferContextual(context -> {
                    assertEquals("user", context.get("rpc-user"));
                    return Mono.fromSupplier(() -> frame.data().retainedSlice()).doOnRequest(demand::addAndGet);
                });
            }
        };
        try {
            StepVerifier.create(RpcServerMono.create(plan, request).contextWrite(Context.of("tenant", "tenant")), 0)
                .then(() -> { assertEquals(0, demand.get()); assertEquals(2, request.refCnt()); assertEquals(1, plan.active.get()); })
                .thenRequest(1).assertNext(RpcServerMonoTest::consume).verifyComplete();
            assertEquals(1, demand.get()); assertEquals(1, request.refCnt()); assertEquals(0, plan.active.get());
        } finally { request.release(); }
    }

    @Test public void emptyAndErrorResponsesWaitForDemandAndHoldTheSlotUntilDeliveredOrCancelled() {
        for (boolean error : new boolean[]{false, true}) for (boolean cancel : new boolean[]{false, true}) {
            RpcFrame request = request(); Plan plan = new Plan() {
                Mono<?> invoke(RpcFrame frame, RpcServerMono.CallSubscriber actual) {
                    return error ? Mono.error(new IllegalStateException("remote failure")) : Mono.empty();
                }
            };
            try {
                var verifier = StepVerifier.create(RpcServerMono.create(plan, request), 0)
                    .then(() -> { assertEquals(1, plan.active.get()); assertEquals(2, request.refCnt()); });
                if (cancel) verifier.thenCancel().verify(WAIT);
                else verifier.thenRequest(1).assertNext(frame -> {
                    try {
                        if (error) expectFailure(RuntimeException.class, () -> RpcProtocol.checkResponse(frame));
                        else assertEquals(0, frame.data().readableBytes());
                    } finally { frame.release(); }
                }).verifyComplete();
                assertEquals(1, request.refCnt()); assertEquals(0, plan.active.get());
            } finally { request.release(); }
        }
    }

    @Test public void cancellationInsideInitializationDoesNotFreeTheBorrowedRequest() {
        RpcFrame request = request(); AtomicInteger subscriptions = new AtomicInteger();
        BaseSubscriber<RpcFrame> actual = new BaseSubscriber<>() {
            protected void hookOnCancel() { request.release(); }
        };
        Plan plan = new Plan() {
            Mono<?> invoke(RpcFrame frame, RpcServerMono.CallSubscriber ignored) {
                actual.dispose(); assertEquals(123, frame.data().getLong(0));
                return Mono.defer(() -> { subscriptions.incrementAndGet(); return Mono.empty(); });
            }
        };
        RpcServerMono.create(plan, request).subscribe(actual);
        assertEquals(0, request.refCnt()); assertEquals(0, subscriptions.get()); assertEquals(0, plan.active.get());
    }

    @Test public void cancelledEncodingReleasesTheProducedFrameAndKeepsTheOwnedValueLiveUntilEncodingEnds() {
        RpcFrame request = request(); AtomicReference<RpcFrame> produced = new AtomicReference<>();
        ByteBuf value = Unpooled.directBuffer(8).writeLong(123);
        BaseSubscriber<RpcFrame> actual = new BaseSubscriber<>() {};
        Plan plan = new Plan() {
            Mono<?> invoke(RpcFrame frame, RpcServerMono.CallSubscriber subscriber) { return Mono.just(value); }
            RpcFrame encode(Object input) {
                actual.dispose(); assertEquals(123, ((ByteBuf) input).getLong(0));
                RpcFrame response = super.encode(input); produced.set(response); return response;
            }
        };
        try {
            RpcServerMono.create(plan, request).subscribe(actual);
            assertEquals(0, value.refCnt()); assertEquals(0, produced.get().refCnt());
            assertEquals(1, request.refCnt()); assertEquals(0, plan.active.get());
        } finally { request.release(); }
    }

    @Test public void failedSlotAndSourceFactoriesAreEncodedAndCleaned() {
        for (boolean acquire : new boolean[]{false, true}) {
            RpcFrame request = request(); Plan plan = new Plan() {
                AtomicInteger acquire() { if (acquire) throw new RejectedExecutionException("slot"); return super.acquire(); }
                Mono<?> invoke(RpcFrame frame, RpcServerMono.CallSubscriber subscriber) { throw new IllegalStateException("factory"); }
            };
            try {
                StepVerifier.create(RpcServerMono.create(plan, request)).assertNext(frame -> {
                    try { expectFailure(RuntimeException.class, () -> RpcProtocol.checkResponse(frame)); }
                    finally { frame.release(); }
                }).verifyComplete();
                assertEquals(1, request.refCnt()); assertEquals(0, plan.active.get());
            } finally { request.release(); }
        }
    }

    @Test public void reusedPublisherCreatesIndependentRequestLeases() {
        RpcFrame request = request(); Plan plan = new Plan() {
            Mono<?> invoke(RpcFrame frame, RpcServerMono.CallSubscriber actual) { return Mono.just(frame.data().retainedSlice()); }
        };
        try {
            Mono<RpcFrame> call = RpcServerMono.create(plan, request);
            for (int i = 0; i < 2; i++) StepVerifier.create(call).assertNext(RpcServerMonoTest::consume).verifyComplete();
            assertEquals(1, request.refCnt()); assertEquals(0, plan.active.get());
        } finally { request.release(); }
    }

    @Test public void concurrentDemandAndCancellationTransferOrReleaseExactlyOneResponse() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 200; i++) {
                RpcFrame request = request(); ByteBuf value = Unpooled.directBuffer(8).writeLong(123);
                AtomicBoolean emitted = new AtomicBoolean(); AtomicReference<RpcFrame> produced = new AtomicReference<>();
                AtomicInteger delivered = new AtomicInteger();
                Plan plan = new Plan() {
                    Mono<?> invoke(RpcFrame frame, RpcServerMono.CallSubscriber actual) {
                        return new Mono<Object>() {
                            public void subscribe(CoreSubscriber<? super Object> subscriber) {
                                subscriber.onSubscribe(new Subscription() {
                                    public void request(long n) { if (emitted.compareAndSet(false, true)) { subscriber.onNext(value); subscriber.onComplete(); } }
                                    public void cancel() { }
                                });
                            }
                        };
                    }
                    RpcFrame encode(Object input) { RpcFrame frame = super.encode(input); produced.set(frame); return frame; }
                };
                BaseSubscriber<RpcFrame> actual = new BaseSubscriber<>() {
                    protected void hookOnSubscribe(Subscription subscription) { }
                    protected void hookOnNext(RpcFrame frame) { delivered.incrementAndGet(); consume(frame); }
                };
                try {
                    RpcServerMono.create(plan, request).subscribe(actual);
                    CyclicBarrier start = new CyclicBarrier(2);
                    Future<?> demand = executor.submit(() -> { await(start); actual.request(1); });
                    Future<?> cancel = executor.submit(() -> { await(start); actual.dispose(); });
                    demand.get(5, TimeUnit.SECONDS); cancel.get(5, TimeUnit.SECONDS);
                    if (!emitted.get()) value.release();
                    assertEquals(0, value.refCnt()); assertTrue(delivered.get() <= 1);
                    if (produced.get() != null) assertEquals(0, produced.get().refCnt());
                    assertEquals(0, plan.active.get()); assertEquals(1, request.refCnt());
                } finally { request.release(); }
            }
        } finally { executor.shutdownNow(); }
    }
    static void await(CyclicBarrier start) {
        try { start.await(5, TimeUnit.SECONDS); } catch (Exception error) { throw new AssertionError(error); }
    }
}
