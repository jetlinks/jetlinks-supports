package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import io.rsocket.*;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.transport.netty.server.CloseableChannel;
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
        RSocket handler = echo(Sinks.one());
        CloseableChannel server = transport.bind(URI.create("tcp://127.0.0.1:0"), 65536, SocketAcceptor.with(handler)).block(WAIT);
        RSocket client = transport.connect(address(server), 65536, allocator::payload, SocketAcceptor.with(handler)).block(WAIT);
        Payload request = ByteBufPayload.create(allocator.directBuffer(32768).writeZero(32768), allocator.directBuffer(32768).writeZero(32768));
        try {
            Payload response = client.requestResponse(ByteBufPayload.create(request)).block(WAIT);
            try { assertEquals(32768, response.data().readableBytes()); assertEquals(32768, response.metadata().readableBytes()); }
            finally { response.release(); }
            assertEquals(1, request.refCnt());
        } finally { request.release(); transport.close().block(WAIT); }
        allocator.assertReleased();
    }
    @Test public void incomingRequestAndResponseBudgetsBoundFragmentReassembly() {
        for (boolean oversizedResponse : new boolean[]{false, true}) {
            Allocator allocator = new Allocator(); RSocketRpcTransport transport = new RSocketRpcTransport(2, allocator);
            var largeReply = new java.util.concurrent.atomic.AtomicBoolean();
            var entered = new java.util.concurrent.atomic.AtomicInteger();
            RSocket handler = new RSocket() {
                public Mono<Payload> requestResponse(Payload request) {
                    entered.incrementAndGet();
                    if (!largeReply.get()) return Mono.just(request);
                    request.release();
                    return Mono.just(ByteBufPayload.create(allocator.directBuffer(262144).writeZero(262144)));
                }
            };
            CloseableChannel server = transport.bind(URI.create("tcp://127.0.0.1:0"), oversizedResponse ? 1048576 : 32768, SocketAcceptor.with(handler)).block(WAIT);
            RSocket client = transport.connect(address(server), oversizedResponse ? 32768 : 1048576, allocator::payload, SocketAcceptor.with(handler)).block(WAIT);
            Payload request = ByteBufPayload.create(allocator.directBuffer(oversizedResponse ? 8 : 262144).writeZero(oversizedResponse ? 8 : 262144));
            try {
                consume(client.requestResponse(allocator.payload()).block(WAIT)); entered.set(0); largeReply.set(oversizedResponse);
                StepVerifier.create(client.requestResponse(ByteBufPayload.create(request))).expectError().verify(WAIT);
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
        Payload payload() { return ByteBufPayload.create(directBuffer(8).writeLong(123)); }
        void assertReleased() { assertFalse(roots.isEmpty()); assertEquals(0, roots.stream().filter(root -> root.refCnt() != 0).count()); }
    }
    static RSocket echo(Sinks.One<Void> notified) {
        return new RSocket() {
            public Mono<Payload> requestResponse(Payload request) { return Mono.just(request); }
            public Flux<Payload> requestStream(Payload request) {
                return Flux.using(() -> request,
                    payload -> Flux.range(0, 32).map(i -> ByteBufPayload.create(payload)), Payload::release, false);
            }
            public Flux<Payload> requestChannel(Publisher<Payload> request) { return Flux.from(request); }
            public Mono<Void> fireAndForget(Payload request) {
                return Mono.using(() -> request, payload -> Mono.fromRunnable(() -> notified.tryEmitEmpty()), Payload::release, false);
            }
        };
    }
    static void consume(Payload payload) {
        try { assertTrue(payload.data().isDirect()); assertEquals(123, payload.data().readLong()); }
        finally { payload.release(); }
    }
    @Test public void oneTcpConnectionSupportsBothDirectionsAndAllInteractionsWithoutRetainedRoots() {
        Allocator allocator = new Allocator(); RSocketRpcTransport transport = new RSocketRpcTransport(2, allocator);
        Sinks.One<RSocket> reverse = Sinks.one(); Sinks.One<Void> notified = Sinks.one();
        RSocket handler = echo(notified);
        CloseableChannel server = transport.bind(URI.create("tcp://127.0.0.1:0"), 64*1024, (setup, connection) -> {
            reverse.tryEmitValue(connection); return Mono.just(handler);
        }).block(WAIT);
        RSocket client = transport.connect(address(server), 64*1024, allocator::payload, SocketAcceptor.with(handler)).block(WAIT);
        try {
            consume(client.requestResponse(allocator.payload()).block(WAIT));
            consume(reverse.asMono().block(WAIT).requestResponse(allocator.payload()).block(WAIT));
            assertEquals(32L, client.requestStream(allocator.payload()).doOnNext(RSocketRpcTransportTest::consume).count().block(WAIT).longValue());
            assertEquals(3L, client.requestStream(allocator.payload()).doOnNext(RSocketRpcTransportTest::consume).take(3).count().block(WAIT).longValue());
            for (int count : new int[]{32,3}) assertEquals(count, client.requestChannel(Flux.range(0,32).map(i -> allocator.payload()))
                .doOnNext(RSocketRpcTransportTest::consume).take(count).count().block(WAIT).intValue());
            client.fireAndForget(allocator.payload()).block(WAIT); notified.asMono().block(WAIT);
            client.dispose();
            RSocket inbound = reverse.asMono().block(WAIT);
            inbound.onClose().onErrorComplete().block(WAIT);
            inbound.dispose(); inbound.dispose();
        } finally { transport.close().block(WAIT); }
        allocator.assertReleased();
    }
    @Test public void rejectedSetupAndRequestsReturnTheirBuffersWithoutReleasingCallerReference() {
        Allocator allocator = new Allocator(); RSocketRpcTransport transport = new RSocketRpcTransport(2, allocator);
        CloseableChannel server = transport.bind(URI.create("tcp://127.0.0.1:0"),64*1024,
            (setup,reverse) -> Mono.error(new IllegalArgumentException("rejected"))).block(WAIT);
        Payload owner = allocator.payload();
        try {
            Mono<Payload> response = transport.connect(address(server),64*1024,() -> ByteBufPayload.create(owner),SocketAcceptor.with(echo(Sinks.one())))
                .flatMap(connection -> connection.requestResponse(allocator.payload()));
            StepVerifier.create(response).expectError().verify(WAIT);
            assertEquals(1, owner.refCnt()); assertEquals(123, owner.data().getLong(0));
        } finally { owner.release(); transport.close().block(WAIT); }
        allocator.assertReleased();
    }
}
