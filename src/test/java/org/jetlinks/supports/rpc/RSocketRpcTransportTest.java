package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.jetlinks.core.rpc.transport.*;
import org.reactivestreams.Publisher;
import org.jetlinks.supports.rpc.rsocket.RSocketRpcTransport;
import org.junit.Test;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.junit.Assert.*;
import static org.jetlinks.supports.rpc.RpcSpiTestSupport.*;

public class RSocketRpcTransportTest {
    @Test public void fragmentedDataAndMetadataAtTheExactBudgetRemainValid() {
        Allocator allocator = new Allocator(); RSocketRpcTransport transport = new RSocketRpcTransport(2, allocator);
        RpcHandler handler = echo(allocator, Sinks.one());
        RpcServer server = transport.bind(URI.create("tcp://127.0.0.1:0"), 65536, (setup, reverse) -> Mono.just(handler)).block(WAIT);
        RpcConnection client = transport.connect(server.address(), 65536, allocator::frame, handler).block(WAIT);
        RpcFrame request = new RpcFrame(allocator.directBuffer(32768).writeZero(32768), allocator.directBuffer(32768).writeZero(32768));
        try {
            RpcFrame response = client.requestResponse(request::retainedDuplicate).block(WAIT);
            try { assertEquals(32768, response.data().readableBytes()); assertEquals(32768, response.metadata().readableBytes()); }
            finally { response.release(); }
            assertEquals(1, request.refCnt());
        } finally { request.release(); transport.close().block(WAIT); }
        allocator.assertReleased();
    }

    @Test public void incomingRequestAndResponseBudgetsBoundFragmentReassembly() {
        for (boolean oversizedResponse : new boolean[]{false, true}) {
            Allocator allocator = new Allocator(); RSocketRpcTransport transport = new RSocketRpcTransport(2, allocator);
            java.util.concurrent.atomic.AtomicBoolean largeReply = new java.util.concurrent.atomic.AtomicBoolean();
            java.util.concurrent.atomic.AtomicInteger entered = new java.util.concurrent.atomic.AtomicInteger();
            RpcHandler handler = new RpcHandler() {
                public Mono<RpcFrame> requestResponse(RpcFrame request) {
                    entered.incrementAndGet();
                    return Mono.just(largeReply.get() ? new RpcFrame(allocator.directBuffer(262144).writeZero(262144), Unpooled.EMPTY_BUFFER) : request.retainedDuplicate());
                }
                public Flux<RpcFrame> requestStream(RpcFrame request) { return Flux.empty(); }
                public Flux<RpcFrame> requestChannel(Publisher<RpcFrame> request) { return Flux.from(request); }
                public Mono<Void> fireAndForget(RpcFrame request) { return Mono.empty(); }
            };
            RpcServer server = transport.bind(URI.create("tcp://127.0.0.1:0"), oversizedResponse ? 1048576 : 32768, (setup, reverse) -> Mono.just(handler)).block(WAIT);
            RpcConnection client = transport.connect(server.address(), oversizedResponse ? 32768 : 1048576, allocator::frame, handler).block(WAIT);
            RpcFrame request = new RpcFrame(allocator.directBuffer(oversizedResponse ? 8 : 262144).writeZero(oversizedResponse ? 8 : 262144), Unpooled.EMPTY_BUFFER);
            try {
                consume(client.requestResponse(allocator::frame).block(WAIT)); entered.set(0); largeReply.set(oversizedResponse);
                StepVerifier.create(client.requestResponse(request::retainedDuplicate)).expectError().verify(WAIT);
                assertEquals(oversizedResponse ? 1 : 0, entered.get()); assertEquals(1, request.refCnt());
            } finally { request.release(); transport.close().block(WAIT); }
            allocator.assertReleased();
        }
    }
    static class Allocator extends AbstractByteBufAllocator {
        final List<ByteBuf> roots = new CopyOnWriteArrayList<>();
        Allocator() { super(true); }
        protected ByteBuf newHeapBuffer(int initial, int max) { return record(UnpooledByteBufAllocator.DEFAULT.heapBuffer(initial, max)); }
        protected ByteBuf newDirectBuffer(int initial, int max) { return record(UnpooledByteBufAllocator.DEFAULT.directBuffer(initial, max)); }
        private ByteBuf record(ByteBuf value) { if (value.capacity() > 0) roots.add(value); return value; }
        public boolean isDirectBufferPooled() { return false; }
        RpcFrame frame() { return new RpcFrame(directBuffer(8).writeLong(123), Unpooled.EMPTY_BUFFER); }
        void assertReleased() { assertFalse(roots.isEmpty()); assertEquals(0, roots.stream().filter(root -> root.refCnt() != 0).count()); }
    }
    static RpcHandler echo(Allocator allocator, Sinks.One<Void> notified) {
        return new RpcHandler() {
            public Mono<RpcFrame> requestResponse(RpcFrame request) { return Mono.just(request.retainedDuplicate()); }
            public Flux<RpcFrame> requestStream(RpcFrame request) {
                return Flux.range(0, 32).map(i -> request.retainedDuplicate()).doOnDiscard(RpcFrame.class, RpcFrame::release);
            }
            public Flux<RpcFrame> requestChannel(Publisher<RpcFrame> request) { return Flux.from(request); }
            public Mono<Void> fireAndForget(RpcFrame request) { notified.tryEmitEmpty(); return Mono.empty(); }
        };
    }
    static void consume(RpcFrame frame) {
        try { assertTrue(frame.data().isDirect()); assertEquals(123, frame.data().readLong()); }
        finally { frame.release(); }
    }

    @Test public void oneTcpConnectionSupportsBothDirectionsAndAllInteractionsWithoutRetainedRoots() {
        Allocator allocator = new Allocator();
        RSocketRpcTransport transport = new RSocketRpcTransport(2, allocator);
        Sinks.One<RpcConnection> reverse = Sinks.one();
        Sinks.One<Void> notified = Sinks.one();
        RpcHandler handler = echo(allocator, notified);
        RpcServer server = transport.bind(URI.create("tcp://127.0.0.1:0"), 64 * 1024, (setup, connection) -> {
            reverse.tryEmitValue(connection); return Mono.just(handler);
        }).block(WAIT);
        RpcConnection client = transport.connect(server.address(), 64 * 1024, allocator::frame, handler).block(WAIT);
        try {
            consume(client.requestResponse(allocator::frame).block(WAIT));
            consume(reverse.asMono().block(WAIT).requestResponse(allocator::frame).block(WAIT));
            assertEquals(32L, client.requestStream(allocator::frame).doOnNext(RSocketRpcTransportTest::consume).count().block(WAIT).longValue());
            assertEquals(3L, client.requestStream(allocator::frame).doOnNext(RSocketRpcTransportTest::consume).take(3).count().block(WAIT).longValue());
            for (int count : new int[]{32, 3}) assertEquals(count, client.requestChannel(Flux.range(0, 32).map(i -> allocator.frame()))
                .doOnNext(RSocketRpcTransportTest::consume).take(count).count().block(WAIT).intValue());
            client.fireAndForget(allocator::frame).block(WAIT); notified.asMono().block(WAIT);
            client.close().block(WAIT);
            RpcConnection inbound = reverse.asMono().block(WAIT);
            inbound.onClose().onErrorComplete().block(WAIT);
            inbound.close().block(WAIT); inbound.close().block(WAIT);
        } finally { transport.close().block(WAIT); }
        allocator.assertReleased();
    }

    @Test public void rejectedSetupAndRequestsReturnTheirBuffersWithoutReleasingCallerReference() {
        Allocator allocator = new Allocator();
        RSocketRpcTransport transport = new RSocketRpcTransport(2, allocator);
        RpcServer server = transport.bind(URI.create("tcp://127.0.0.1:0"), 64 * 1024, (setup, reverse) -> Mono.error(new IllegalArgumentException("rejected"))).block(WAIT);
        RpcFrame owner = allocator.frame();
        try {
            Mono<RpcFrame> response = transport.connect(server.address(), 64 * 1024, owner::retainedDuplicate, echo(allocator, Sinks.one()))
                .flatMap(connection -> connection.requestResponse(allocator::frame));
            StepVerifier.create(response).expectError().verify(WAIT);
            assertEquals(1, owner.refCnt()); assertEquals(123, owner.data().getLong(0));
        } finally { owner.release(); transport.close().block(WAIT); }
        allocator.assertReleased();
    }
}
