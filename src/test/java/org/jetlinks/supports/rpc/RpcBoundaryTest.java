package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.hswebframework.web.authorization.exception.AccessDenyException;
import org.jetlinks.core.rpc.*;
import org.jetlinks.core.rpc.transport.RpcFrame;
import org.jetlinks.core.rpc.transport.*;
import org.jetlinks.core.cluster.discovery.*;
import org.jetlinks.supports.rpc.rsocket.RSocketRpcTransport;
import org.junit.Test;
import reactor.core.*;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import reactor.util.context.*;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import static org.junit.Assert.*;
import static org.jetlinks.supports.rpc.RpcSpiTestSupport.*;
import static org.jetlinks.supports.rpc.DefaultRpcManagerTest.*;

public class RpcBoundaryTest {
    @Test public void discoveryFailureReachesExistingAndLateWatchersAndRejectsRemoteCalls() {
        try (Fixture fixture = new Fixture()) {
            RuntimeException failure = new IllegalStateException("discovery failed");
            StepVerifier.create(fixture.first.watchServices(Echo.class)).expectNextMatches(view -> view.services().size() == 1)
                .then(() -> fixture.group.view.fail(failure)).expectErrorMatches(error -> error == failure).verify(WAIT);
            StepVerifier.create(fixture.first.watchServices(Echo.class).next()).expectErrorMatches(error -> error == failure).verify(WAIT);
            StepVerifier.create(fixture.first.getServices(Echo.class)).expectErrorMatches(error -> error == failure).verify(WAIT);
            StepVerifier.create(fixture.first.getService("b", Echo.class)).expectErrorMatches(error -> error == failure).verify(WAIT);
            StepVerifier.create(fixture.echo.context("late call")).expectErrorMatches(error -> error == failure).verify(WAIT);
        }
    }

    @Test public void independentRegistriesRoundRobinIndependently() {
        var left = new DefaultRpcServiceRegistry(node("left").identity(), (n,k,m,r) -> q -> Mono.empty(), () -> {});
        var right = new DefaultRpcServiceRegistry(node("right").identity(), (n,k,m,r) -> q -> Mono.empty(), () -> {});
        var b = node("b").identity(); var c = node("c").identity();
        var catalog = java.util.Set.of(new DefaultRpcServiceRegistry.ServiceKey("_default", RpcMethodBinding.APIS.get(Echo.class).name));
        try {
            for (var registry : java.util.List.of(left, right)) { registry.members(java.util.Set.of(b, c)); registry.ready(b, catalog); registry.ready(c, catalog); }
            for (int i = 0; i < 4; i++) {
                String expected = i % 2 == 0 ? "b" : "c";
                assertEquals(expected, left.selectService(Echo.class).block(WAIT).serverNodeId());
                assertEquals(expected, right.selectService(Echo.class).block(WAIT).serverNodeId());
            }
        } finally { left.close(); right.close(); }
    }
    @Test public void failedDiscoveryShutdownStillClosesTheTransport() {
        AtomicInteger closed = new AtomicInteger();
        RpcTransport transport = new RpcTransport() {
            public Mono<RpcServer> bind(URI address, int maxPayloadBytes, Acceptor acceptor) { return Mono.error(new UnsupportedOperationException()); }
            public Mono<RpcConnection> connect(URI address, int maxPayloadBytes, java.util.function.Supplier<RpcFrame> setup, RpcHandler inbound) { return Mono.error(new UnsupportedOperationException()); }
            public Mono<Void> close() { return Mono.fromRunnable(closed::incrementAndGet); }
        };
        NodeDiscovery discovery = new NodeDiscovery() {
            public Mono<Void> start(NodeAnnouncement local) { return Mono.empty(); }
            public ClusterView currentView() { return new ClusterView(0, java.util.Map.of()); }
            public Flux<ClusterView> watch() { return Flux.never(); }
            public Mono<Void> stop() { return Mono.error(new IllegalStateException("discovery shutdown failed")); }
        };
        DefaultRpcManager manager = new DefaultRpcManager(node("a"), discovery, transport, new JavaRpcBodyCodec(),
            ContextCodec.DEFAULT, RpcOptions.defaults(URI.create("tcp://127.0.0.1:0")));
        StepVerifier.create(manager.stop()).expectErrorMessage("discovery shutdown failed").verify(WAIT);
        manager.stop().block(WAIT);
        assertEquals(1, closed.get());
    }
    @Test public void frameViewRetainsOneOwnerAndHasIndependentIndexes() {
        ByteBuf owner = Unpooled.directBuffer(8).writeLong(123);
        RpcFrame frame = new RpcFrame(owner.retainedSlice(), Unpooled.EMPTY_BUFFER);
        RpcFrame child = frame.retainedDuplicate();
        assertEquals(123, child.data().readLong());
        assertEquals(0, frame.data().readerIndex());
        frame.release(); child.release();
        assertEquals(1, owner.refCnt());
        try { frame.data(); fail("released frame must reject access"); }
        catch (io.netty.util.IllegalReferenceCountException expected) { assertEquals(0, frame.refCnt()); }
        owner.release();
        ByteBuf shared = Unpooled.directBuffer(8).writeLong(123);
        RpcFrame empty = new RpcFrame(shared.retainedSlice(0, 0), shared.retainedSlice(0, 0));
        shared.release(); empty.release();
        assertEquals(0, shared.refCnt());
    }

    @Test public void securityDecodeRejectsBeforeEnteringSynchronousApiBody() {
        CountingContext sender = new CountingContext();
        ContextCodec denied = new ContextCodec() {
            public String formatId() { return sender.formatId(); }
            public SerializedContext serialize(ContextView context) { return sender.serialize(context); }
            public Context deserializeFrom(Context base, ByteBuf input) { throw new AccessDenyException.NoStackTrace("denied"); }
        };
        try (Fixture fixture = new Fixture(sender, denied)) {
            StepVerifier.create(fixture.echo.context("denied").contextWrite(Context.of("user", "test")))
                .expectError(AccessDenyException.class).verify(WAIT);
            assertEquals(0, fixture.implementation.entered.get());
            await(() -> sender.captured.get() == sender.disposed.get());
        }
    }

    @Test public void failedContextEncodingReleasesBorrowedBodyAndCapturedResources() {
        AtomicInteger disposed = new AtomicInteger();
        ContextCodec failing = new ContextCodec() {
            public String formatId() { return "failed-encode/1"; }
            public SerializedContext serialize(ContextView context) {
                return new SerializedContext() {
                    public String getContext() { throw new AssertionError("Binary RPC must not request a context String"); }
                    public void writeTo(ByteBuf target) { throw new IllegalArgumentException("encode failed"); }
                    public void dispose() { disposed.incrementAndGet(); }
                };
            }
            public Context deserializeFrom(Context base, ByteBuf source) { return base; }
        };
        try (Fixture fixture = new Fixture(failing, failing)) {
            ByteBuf owner = Unpooled.directBuffer(8).writeLong(123);
            try {
                StepVerifier.create(fixture.echo.bytes(owner)).expectError(IllegalArgumentException.class).verify(WAIT);
                assertEquals(1, owner.refCnt()); assertEquals(1, disposed.get()); assertEquals(0, fixture.first.activeCalls());
            } finally { owner.release(); }
        }
    }

    @Test public void inFlightAndFrameBudgetsRejectBeforeAnotherSend() {
        Group group = new Group();
        RpcOptions defaults = RpcOptions.defaults(URI.create("tcp://127.0.0.1:0"));
        RpcOptions limited = new RpcOptions(defaults.listen(), defaults.connectTimeout(), defaults.requestTimeout(), defaults.reconnectDelay(), 1, 1024, "");
        DefaultRpcManager first = new DefaultRpcManager(node("a"), group.discovery(), new RSocketRpcTransport(2),
            new JavaRpcBodyCodec(), ContextCodec.DEFAULT, limited);
        DefaultRpcManager second = manager("b", group.discovery(), ContextCodec.DEFAULT);
        EchoImpl implementation = new EchoImpl() {
            public Mono<ByteBuf> bytes(ByteBuf request) { return Mono.just(Unpooled.directBuffer(2048).writeZero(2048)); }
        };
        second.registerService(implementation);
        first.start().block(WAIT); second.start().block(WAIT);
        ByteBuf pending = Unpooled.directBuffer(8).writeLong(123), other = Unpooled.directBuffer(8).writeLong(123);
        Disposable subscription = null;
        try {
            Echo echo = service(first, "b", Echo.class);
            subscription = echo.pending(pending).subscribe();
            implementation.subscribed.asMono().block(WAIT);
            StepVerifier.create(echo.bytes(other)).expectError(RejectedExecutionException.class).verify(WAIT);
            assertEquals(1, other.refCnt());
            subscription.dispose(); implementation.cancelled.asMono().block(WAIT);
            await(() -> first.activeCalls() == 0 && pending.refCnt() == 1);
            StepVerifier.create(echo.bytes(other)).expectError(IllegalArgumentException.class).verify(WAIT);
            await(() -> other.refCnt() == 1 && first.activeCalls() == 0);
            ByteBuf huge = Unpooled.directBuffer(2048).writeZero(2048);
            try {
                StepVerifier.create(echo.bytes(huge)).expectError(IllegalArgumentException.class).verify(WAIT);
                assertEquals(1, huge.refCnt()); assertEquals(0, first.activeCalls());
            } finally { huge.release(); }
            ByteBuf channel = Unpooled.directBuffer(2048).writeZero(2048);
            StepVerifier.create(echo.channel(Flux.just(channel))).expectError(IllegalArgumentException.class).verify(WAIT);
            assertEquals(0, channel.refCnt());
        } finally {
            if (subscription != null) subscription.dispose();
            pending.release(); other.release(); first.stop().block(WAIT); second.stop().block(WAIT);
        }
    }

    public static class NamedEcho extends EchoImpl {
        private final String name;
        public NamedEcho(String name) { this.name = name; }
        public Mono<String> context(String request) { return Mono.just(name); }
    }
    @Test public void customSelectorRemainsLazyAndReselectsForEachSubscription() {
        Group group = new Group();
        DefaultRpcManager first = manager("a", group.discovery(), ContextCodec.DEFAULT);
        DefaultRpcManager second = manager("b", group.discovery(), ContextCodec.DEFAULT);
        DefaultRpcManager third = manager("c", group.discovery(), ContextCodec.DEFAULT);
        second.registerService(new NamedEcho("b")); third.registerService(new NamedEcho("c"));
        first.start().block(WAIT); second.start().block(WAIT);
        try {
            service(first, "b", Echo.class);
            Echo selected = first.selectService(Echo.class,
                Collectors.maxBy(java.util.Comparator.comparing(RpcService::serverNodeId)), Mono.<Echo>error(new IllegalStateException("no route"))).block(WAIT);
            Mono<String> assembled = selected.context("select");
            assertEquals("b", assembled.block(WAIT));
            third.start().block(WAIT); service(first, "c", Echo.class);
            assertEquals("c", assembled.block(WAIT));
        } finally { first.stop().block(WAIT); second.stop().block(WAIT); third.stop().block(WAIT); }
    }
}
