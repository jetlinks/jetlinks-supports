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
            Object.class, SerializedContext.class);
        factory.setAccessible(true);
        try { return (ServiceMessage) factory.invoke(node, context, info, request, serialized); }
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

    @Test public void legacyUnreleasableArgumentStillGetsAnOwnedCopy() throws Throwable {
        ByteBuf owner = PooledByteBufAllocator.DEFAULT.directBuffer(8).writeLong(123);
        ByteBuf copy = prepare(Unpooled.unreleasableBuffer(owner), Context.empty()).data();
        try {
            assertNotEquals(owner.memoryAddress(), copy.memoryAddress());
            owner.release(); assertEquals(123, copy.readLong());
        } finally { copy.release(); }
        assertEquals(0, owner.refCnt()); assertEquals(0, copy.refCnt());
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

    @Test public void failedLegacyPreparationReleasesItsCopyAsWellAsTheArgument() throws Throwable {
        ByteBuf argument = spy(Unpooled.buffer(8).writeLong(123));
        ByteBuf copy = Unpooled.buffer(8).writeLong(123);
        doReturn(copy).when(argument).copy();
        boolean rejected = false;
        try { prepare(argument, Context.of(ScalecubeRpcManager.CONTEXT_CALL_DEPTH_KEY, 256)); }
        catch (org.hswebframework.web.exception.I18nSupportException expected) { rejected = true; }
        assertTrue(rejected);
        assertEquals(0, argument.refCnt()); assertEquals(0, copy.refCnt());
    }
}
