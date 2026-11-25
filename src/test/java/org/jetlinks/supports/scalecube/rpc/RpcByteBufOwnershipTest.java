package org.jetlinks.supports.scalecube.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.scalecube.services.Reflect;
import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.methods.MethodInfo;
import org.jetlinks.core.rpc.SerializedContext;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import io.scalecube.services.ServiceCall;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import java.lang.reflect.Field;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RpcByteBufOwnershipTest {
    @io.scalecube.services.annotations.Service
    public interface Service {
        @ServiceMethod @RequestType(ByteBuf.class) Mono<ByteBuf> envelope(ServiceMessage request);
        @ServiceMethod Mono<ByteBuf> legacy(ByteBuf request);
        @ServiceMethod Mono<Void> voidResponse(ByteBuf request);
        @ServiceMethod Flux<ByteBuf> stream(ByteBuf request);
        @ServiceMethod Flux<ByteBuf> channel(Flux<ByteBuf> request);
        @ServiceMethod @RequestType(ByteBuf.class) Mono<ServiceMessage> envelopeResponse(ServiceMessage request);
        @ServiceMethod Flux<ServiceMessage> envelopeStream(ByteBuf request);
        @ServiceMethod @RequestType(ByteBuf.class) Flux<ServiceMessage> envelopeChannel(Flux<ServiceMessage> request);
    }

    private ServiceMessage prepare(Object request, ContextView context) throws Throwable {
        return prepare(request, context, SerializedContext.empty());
    }

    private ServiceMessage prepare(Object request, ContextView context, SerializedContext serialized) throws Throwable {
        var rpc = new ScalecubeRpcManager();
        var node = rpc.new ClusterNode();
        Method api = Service.class.getMethod(request instanceof ServiceMessage ? "envelope" : "legacy",
            request instanceof ServiceMessage ? ServiceMessage.class : ByteBuf.class);
        MethodInfo info = Reflect.methodsInfo(Service.class).get(api);
        Method factory = node.getClass().getDeclaredMethod("toServiceMessage0", ContextView.class, MethodInfo.class,
            Object.class, ByteBuf.class, SerializedContext.class);
        factory.setAccessible(true);
        ByteBuf payload = request instanceof ByteBuf buffer ? buffer : ((ServiceMessage) request).data();
        try { return (ServiceMessage) factory.invoke(node, context, info, request, payload.retainedDuplicate(), serialized); }
        catch (InvocationTargetException error) { throw error.getCause(); }
    }

    private Service proxy(ServiceCall transport, Scheduler scheduler) throws Exception {
        return proxy(transport, scheduler, org.jetlinks.core.rpc.ContextCodec.DEFAULT);
    }

    private Service proxy(ServiceCall transport, Scheduler scheduler, org.jetlinks.core.rpc.ContextCodec codec) throws Exception {
        var rpc = new ScalecubeRpcManager();
        rpc.setContextCodec(codec);
        Field field = ScalecubeRpcManager.class.getDeclaredField("requestScheduler");
        field.setAccessible(true); field.set(rpc, scheduler);
        var node = rpc.new ClusterNode();
        Method api = node.getClass().getDeclaredMethod("api", ServiceCall.class, String.class, Class.class);
        api.setAccessible(true);
        return (Service) api.invoke(node, transport, "owned", Service.class);
    }

    @Test public void cancellationBeforeScheduledEncodingDoesNotLeakOrSend() throws Exception {
        var queue = new java.util.concurrent.ConcurrentLinkedQueue<Runnable>();
        Scheduler scheduler = Schedulers.fromExecutor(queue::add);
        var transport = mock(ServiceCall.class);
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        var error = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        try {
            Service service = proxy(transport, scheduler);
            var subscription = Mono.using(() -> owner,
                data -> service.envelope(ServiceMessage.builder().data(data).build()), ByteBuf::release)
                .subscribe(ByteBuf::release, error::set);
            assertFalse(queue.isEmpty()); subscription.dispose();
            for (Runnable task; (task = queue.poll()) != null;) task.run();
            assertNull(error.get()); assertEquals(0, owner.refCnt()); verifyNoInteractions(transport);
        } finally { scheduler.dispose(); if (owner.refCnt() > 0) owner.release(); }
    }

    @Test public void bareRequestIsRetainedBeforeSchedulingAndReleasedOnCancellation() throws Exception {
        var queue = new java.util.concurrent.ConcurrentLinkedQueue<Runnable>();
        Scheduler scheduler = Schedulers.fromExecutor(queue::add);
        var transport = mock(ServiceCall.class);
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        reactor.core.Disposable subscription = null;
        try {
            subscription = proxy(transport, scheduler).legacy(owner).subscribe(ByteBuf::release);
            assertEquals(2, owner.refCnt());
            subscription.dispose(); assertEquals(1, owner.refCnt());
            for (Runnable task; (task = queue.poll()) != null;) task.run();
            verifyNoInteractions(transport);
        } finally {
            if (subscription != null) subscription.dispose();
            scheduler.dispose(); owner.release();
        }
        assertEquals(0, owner.refCnt());
    }

    @Test public void bareRequestPreparationBorrowsWithoutCopyOrConsumption() throws Throwable {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf lease = null;
        try {
            lease = prepare(owner, Context.empty()).data();
            assertEquals(2, owner.refCnt()); assertEquals(owner.memoryAddress(), lease.memoryAddress());
            assertEquals(123, lease.readLong()); assertEquals(0, owner.readerIndex());
        } finally { if (lease != null) lease.release(); if (owner.refCnt() > 0) owner.release(); }
        assertEquals(0, owner.refCnt());
    }

    @Test public void callSnapshotsOnlyTheReadableRegionWithoutCopyingIt() throws Exception {
        var transport = mock(ServiceCall.class);
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(64)
            .writeInt(999).writeLong(123).writeLong(456).writeInt(888).setIndex(4, 20);
        when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
            ByteBuf data = ((ServiceMessage) call.getArgument(0)).data();
            try {
                assertEquals(0, data.readerIndex()); assertEquals(16, data.writerIndex()); assertEquals(16, data.capacity());
                assertEquals(owner.memoryAddress() + 4, data.memoryAddress());
                assertEquals(123, data.readLong()); assertEquals(456, data.readLong());
                assertEquals(4, owner.readerIndex()); assertEquals(20, owner.writerIndex());
            } finally { data.release(); }
            return Mono.just(ServiceMessage.builder().data(Unpooled.EMPTY_BUFFER).build());
        });
        try {
            proxy(transport, Schedulers.immediate()).legacy(owner).block().release();
            assertEquals(1, owner.refCnt());
        } finally { owner.release(); }
        assertEquals(0, owner.refCnt());
    }

    @Test public void unreleasableDuplicateCannotProtectAgainstCallerCancellation() {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf notOwned = Unpooled.unreleasableBuffer(owner).retainedDuplicate();
        assertEquals(1, owner.refCnt()); owner.release();
        try { notOwned.readLong(); fail("unreleasable duplicate must not keep the allocation alive"); }
        catch (io.netty.util.IllegalReferenceCountException expected) { assertEquals(0, notOwned.refCnt()); }
    }

    @Test public void cancellationDoesNotInvalidateAnAttemptStillBeingEncoded() throws Exception {
        ServiceCall transport = mock(ServiceCall.class);
        var pending = new java.util.concurrent.atomic.AtomicReference<ByteBuf>();
        when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
            pending.set(((ServiceMessage) call.getArgument(0)).data());
            return Mono.<ServiceMessage>never();
        });
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf lease = null;
        try {
            Service service = proxy(transport, Schedulers.immediate());
            var subscription = Mono.using(() -> owner, service::legacy, ByteBuf::release).subscribe();
            lease = pending.get(); assertNotNull(lease); assertEquals(owner.memoryAddress(), lease.memoryAddress());
            subscription.dispose();
            assertEquals(123, lease.readLong());
            lease.release(); lease = null; assertEquals(0, owner.refCnt());
        } finally { if (lease != null) lease.release(); if (owner.refCnt() > 0) owner.release(); }
    }

    @Test public void bareRequestRetryGetsIndependentAttemptIndexes() throws Exception {
        ServiceCall transport = mock(ServiceCall.class);
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
            ByteBuf data = ((ServiceMessage) call.getArgument(0)).data();
            assertEquals(owner.memoryAddress(), data.memoryAddress());
            assertEquals(0, data.readerIndex()); assertEquals(123, data.readLong()); data.release();
            return attempts.incrementAndGet() == 1 ? Mono.error(new IllegalStateException("retry")) :
                Mono.just(ServiceMessage.builder().data(Unpooled.buffer(8).writeLong(456)).build());
        });
        Service service = proxy(transport, Schedulers.immediate());
        try {
            ByteBuf response = service.legacy(owner)
                .contextWrite(Context.of(reactor.util.retry.Retry.class, reactor.util.retry.Retry.max(0))).retry(1).block();
            try { assertEquals(456, response.readLong()); } finally { response.release(); }
            assertEquals(2, attempts.get()); assertEquals(0, owner.readerIndex()); assertEquals(1, owner.refCnt());
        } finally { owner.release(); }
        assertEquals(0, owner.refCnt());
    }

    @Test public void synchronousContextFailureReturnsTheCallReference() throws Exception {
        var codec = new org.jetlinks.core.rpc.ContextCodec() {
            @Override public SerializedContext serialize(ContextView context) { throw new IllegalStateException("context"); }
        };
        var transport = mock(ServiceCall.class);
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        try {
            Service service = proxy(transport, Schedulers.immediate(), codec);
            reactor.test.StepVerifier.create(service.legacy(owner)).expectErrorMessage("context").verify();
            assertEquals(1, owner.refCnt()); verifyNoInteractions(transport);
        } finally { owner.release(); }
    }

    @Test public void voidResponseAndStreamAlsoBorrowWithoutConsumingTheCaller() throws Exception {
        var transport = mock(ServiceCall.class);
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        java.util.function.Consumer<ServiceMessage> consume = message -> {
            ByteBuf data = message.data();
            try { assertEquals(owner.memoryAddress(), data.memoryAddress()); assertEquals(123, data.readLong()); }
            finally { data.release(); }
        };
        when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class)))
            .thenAnswer(call -> { consume.accept(call.getArgument(0)); return Mono.empty(); });
        when(transport.requestMany(any(ServiceMessage.class), any(java.lang.reflect.Type.class)))
            .thenAnswer(call -> { consume.accept(call.getArgument(0)); return Flux.empty(); });
        try {
            Service service = proxy(transport, Schedulers.immediate());
            service.voidResponse(owner).block(); service.stream(owner).blockLast();
            assertEquals(0, owner.readerIndex()); assertEquals(1, owner.refCnt());
        } finally { owner.release(); }
    }

    @Test public void channelTransfersEachEmittedReferenceWithoutAnExtraRetain() throws Exception {
        var transport = mock(ServiceCall.class);
        ByteBuf first = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf second = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(456);
        var index = new java.util.concurrent.atomic.AtomicInteger();
        when(transport.requestBidirectional(any(org.reactivestreams.Publisher.class), any(java.lang.reflect.Type.class)))
            .thenAnswer(call -> Flux.from((org.reactivestreams.Publisher<ServiceMessage>) call.getArgument(0)).map(message -> {
                ByteBuf data = message.data();
                assertSame(index.getAndIncrement() == 0 ? first : second, data);
                data.release(); return ServiceMessage.builder().data(Unpooled.EMPTY_BUFFER).build();
            }));
        Service service = proxy(transport, Schedulers.immediate());
        service.channel(Flux.just(first, second)).then().block();
        assertEquals(2, index.get()); assertEquals(0, first.refCnt()); assertEquals(0, second.refCnt());
    }

    @Test public void channelCancellationReleasesTheQueuedUnsentBuffer() throws Exception {
        var transport = mock(ServiceCall.class);
        ByteBuf first = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf second = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(456);
        when(transport.requestBidirectional(any(org.reactivestreams.Publisher.class), any(java.lang.reflect.Type.class)))
            .thenAnswer(call -> Flux.from((org.reactivestreams.Publisher<ServiceMessage>) call.getArgument(0)).take(1).map(message -> {
                ((ByteBuf) message.data()).release();
                return ServiceMessage.builder().data(Unpooled.EMPTY_BUFFER).build();
            }));
        Service service = proxy(transport, Schedulers.immediate());
        service.channel(Flux.just(first, second).publishOn(Schedulers.immediate(), 2)).then().block();
        assertEquals(0, first.refCnt()); assertEquals(0, second.refCnt());
    }

    @Test public void channelCancellationAlsoReleasesQueuedServiceMessages() throws Exception {
        var transport = mock(ServiceCall.class);
        ByteBuf first = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf second = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(456);
        when(transport.requestBidirectional(any(org.reactivestreams.Publisher.class), any(java.lang.reflect.Type.class)))
            .thenAnswer(call -> Flux.from((org.reactivestreams.Publisher<ServiceMessage>) call.getArgument(0)).take(1).map(message -> {
                ((ByteBuf) message.data()).release();
                return ServiceMessage.builder().build();
            }));
        Service service = proxy(transport, Schedulers.immediate());
        service.envelopeChannel(Flux.just(ServiceMessage.builder().data(first).build(), ServiceMessage.builder().data(second).build())
            .publishOn(Schedulers.immediate(), 2)).then().block();
        assertEquals(0, first.refCnt()); assertEquals(0, second.refCnt());
    }

    @Test public void lateResponseAfterCancellationIsDiscardedForBothReturnShapes() throws Exception {
        for (boolean envelope : new boolean[]{false, true}) {
            var transport = mock(ServiceCall.class);
            var pending = new java.util.concurrent.atomic.AtomicReference<reactor.core.publisher.MonoSink<ServiceMessage>>();
            when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
                ((ByteBuf) ((ServiceMessage) call.getArgument(0)).data()).release();
                return Mono.<ServiceMessage>create(pending::set);
            });
            ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
            ByteBuf reply = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(456);
            try {
                Service service = proxy(transport, Schedulers.immediate());
                Mono<?> call = envelope ? service.envelopeResponse(ServiceMessage.builder().data(owner).build()) : service.legacy(owner);
                var subscription = call.subscribe(); subscription.dispose();
                pending.get().success(ServiceMessage.builder().data(reply).build());
                assertEquals(0, reply.refCnt()); assertEquals(1, owner.refCnt());
            } finally { owner.release(); if (reply.refCnt() > 0) reply.release(); }
        }
    }

    @Test public void envelopeReturnKeepsIdentityAndDoesNotSerializeDiscardHooks() throws Exception {
        var transport = mock(ServiceCall.class);
        var codec = new org.jetlinks.core.rpc.ContextCodec() {
            @Override public SerializedContext serialize(ContextView context) {
                assertEquals("preserved", context.get("caller"));
                assertFalse(context.hasKey("reactor.onDiscard.local"));
                return SerializedContext.empty();
            }
        };
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf reply = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(456);
        ServiceMessage response = ServiceMessage.builder().header("reply", "kept").data(reply).build();
        java.util.function.Consumer<ServiceMessage> consume = message -> ((ByteBuf) message.data()).release();
        when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
            consume.accept(call.getArgument(0)); return Mono.just(response);
        });
        when(transport.requestMany(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
            consume.accept(call.getArgument(0)); return Flux.just(response);
        });
        try {
            Service service = proxy(transport, Schedulers.immediate(), codec);
            assertSame(response, service.envelopeResponse(ServiceMessage.builder().data(owner).build())
                .contextWrite(Context.of("caller", "preserved")).block());
            assertSame(response, service.envelopeStream(owner).contextWrite(Context.of("caller", "preserved")).blockLast());
            assertEquals("kept", response.header("reply")); assertEquals(456, reply.readLong()); assertEquals(1, owner.refCnt());
        } finally { owner.release(); reply.release(); }
    }

    @Test public void cancellationAfterPreparationReleasesTheTransportLease() throws Exception {
        ServiceCall transport = mock(ServiceCall.class);
        var sent = new java.util.concurrent.atomic.AtomicReference<ByteBuf>();
        when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
            ByteBuf data = ((ServiceMessage) call.getArgument(0)).data(); sent.set(data);
            return Mono.<ServiceMessage>never().doOnCancel(data::release);
        });
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        Service service = proxy(transport, Schedulers.immediate());
        var subscription = Mono.using(() -> owner,
            data -> service.envelope(ServiceMessage.builder().data(data).build()), ByteBuf::release).subscribe();
        assertNotNull(sent.get()); assertEquals(owner.memoryAddress(), sent.get().memoryAddress());
        subscription.dispose(); assertEquals(0, owner.refCnt()); assertEquals(0, sent.get().refCnt());
    }

    @Test public void retryBorrowsANewReaderWithoutConsumingTheCallerBuffer() throws Exception {
        ServiceCall transport = mock(ServiceCall.class);
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        when(transport.requestOne(any(ServiceMessage.class), any(java.lang.reflect.Type.class))).thenAnswer(call -> {
            ByteBuf data = ((ServiceMessage) call.getArgument(0)).data();
            assertEquals(0, data.readerIndex()); assertEquals(123, data.readLong()); data.release();
            return attempts.incrementAndGet() == 1 ? Mono.error(new IllegalStateException("retry")) :
                Mono.just(ServiceMessage.builder().data(PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(456)).build());
        });
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        Service service = proxy(transport, Schedulers.immediate());
        ByteBuf response = Mono.using(() -> owner, data -> service.envelope(ServiceMessage.builder().data(data).build())
            .contextWrite(Context.of(reactor.util.retry.Retry.class, reactor.util.retry.Retry.max(0))).retry(1), ByteBuf::release).block();
        try { assertEquals(456, response.readLong()); } finally { response.release(); }
        assertEquals(2, attempts.get()); assertEquals(0, owner.refCnt());
    }

    @Test public void envelopeLeasesShareBytesAndHaveIndependentIndexes() throws Throwable {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(16).writeLong(123).writeLong(456);
        ByteBuf first = null, second = null;
        try {
            var request = ServiceMessage.builder().data(owner).build();
            first = prepare(request, Context.empty()).data();
            second = prepare(request, Context.empty()).data();
            assertEquals(owner.memoryAddress(), first.memoryAddress());
            assertEquals(123, first.readLong()); assertEquals(0, second.readerIndex()); assertEquals(0, owner.readerIndex());
            owner.release(); // Caller cancellation must not invalidate either in-flight transport lease.
            assertEquals(456, first.readLong()); assertEquals(123, second.readLong());
            first.release(); first = null;
            assertEquals(456, second.readLong()); second.release(); second = null;
            assertEquals(0, owner.refCnt());
        } finally { if (first != null) first.release(); if (second != null) second.release(); if (owner.refCnt() > 0) owner.release(); }
    }

    @Test public void callerReleaseDuringHeaderPreparationCannotInvalidateTheLease() throws Exception {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var resume = new java.util.concurrent.CountDownLatch(1);
        var result = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                return prepare(ServiceMessage.builder().data(owner).build(), Context.empty(), () -> {
                    entered.countDown();
                    try { if (!resume.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("header wait timed out"); }
                    catch (InterruptedException error) { throw new IllegalStateException(error); }
                    return null;
                });
            } catch (Throwable error) { throw new java.util.concurrent.CompletionException(error); }
        });
        try {
            assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)); assertEquals(2, owner.refCnt());
            owner.release();
        } finally { resume.countDown(); }
        ByteBuf lease = result.get(5, java.util.concurrent.TimeUnit.SECONDS).data();
        try { assertEquals(123, lease.readLong()); } finally { lease.release(); }
        assertEquals(0, owner.refCnt());
    }

    @Test public void cancellationDuringHeaderPreparationDiscardsTheUnsentLease() throws Exception {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var resume = new java.util.concurrent.CompletableFuture<Void>();
        var finished = new java.util.concurrent.CountDownLatch(1);
        var transport = mock(ServiceCall.class);
        Scheduler scheduler = Schedulers.newSingle("rpc-ownership-test");
        var codec = new org.jetlinks.core.rpc.ContextCodec() {
            @Override public SerializedContext serialize(ContextView context) {
                return () -> { entered.countDown(); resume.join(); return null; };
            }
        };
        var errors = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        try {
            Service service = proxy(transport, scheduler, codec);
            var subscription = Mono.using(() -> owner,
                data -> service.envelope(ServiceMessage.builder().data(data).build()), ByteBuf::release)
                .subscribe(ByteBuf::release, errors::set);
            try {
                assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
                subscription.dispose(); assertEquals(1, owner.refCnt());
            } finally { resume.complete(null); }
            scheduler.schedule(finished::countDown);
            assertTrue(finished.await(5, java.util.concurrent.TimeUnit.SECONDS));
            assertNull(errors.get()); assertEquals(0, owner.refCnt()); verifyNoInteractions(transport);
        } finally { resume.complete(null); scheduler.dispose(); if (owner.refCnt() > 0) owner.release(); }
    }

    @Test public void slicedCompositeLeaseSurvivesBothCallerReleases() throws Throwable {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.compositeBuffer(2).addComponents(true,
            Unpooled.wrappedBuffer(new byte[]{1, 2, 3}), Unpooled.wrappedBuffer(new byte[]{4, 5, 6}));
        ByteBuf slice = owner.retainedSlice(1, 4);
        ByteBuf lease = prepare(ServiceMessage.builder().data(slice).build(), Context.empty()).data();
        owner.release(); slice.release();
        try { byte[] actual = new byte[4]; lease.readBytes(actual); assertArrayEquals(new byte[]{2, 3, 4, 5}, actual); }
        finally { lease.release(); }
        assertEquals(0, owner.refCnt());
    }

    @Test public void bareRequestLeaseSurvivesTheCallerRelease() throws Throwable {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf lease = prepare(owner, Context.empty()).data();
        try {
            assertEquals(owner.memoryAddress(), lease.memoryAddress());
            owner.release(); assertEquals(123, lease.readLong());
        } finally { lease.release(); }
        assertEquals(0, owner.refCnt()); assertEquals(0, lease.refCnt());
    }

    @Test public void failedEnvelopePreparationDoesNotRetainTheBorrowedOwner() throws Throwable {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        try {
            boolean rejected = false;
            try { prepare(ServiceMessage.builder().data(owner).build(), Context.of(ScalecubeRpcManager.CONTEXT_CALL_DEPTH_KEY, 256)); }
            catch (org.hswebframework.web.exception.I18nSupportException expected) { rejected = true; }
            assertTrue(rejected);
            assertEquals(1, owner.refCnt());
        } finally { owner.release(); }
    }

    @Test public void failedBarePreparationReleasesOnlyTheAttemptReference() throws Throwable {
        ByteBuf argument = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        try {
            boolean rejected = false;
            try { prepare(argument, Context.of(ScalecubeRpcManager.CONTEXT_CALL_DEPTH_KEY, 256)); }
            catch (org.hswebframework.web.exception.I18nSupportException expected) { rejected = true; }
            assertTrue(rejected); assertEquals(1, argument.refCnt());
        } finally { argument.release(); }
        assertEquals(0, argument.refCnt());
    }
}
