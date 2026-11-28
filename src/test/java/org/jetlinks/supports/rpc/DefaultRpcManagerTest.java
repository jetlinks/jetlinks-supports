package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.jetlinks.core.rpc.*;
import org.jetlinks.core.rpc.annotation.*;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import reactor.util.context.*;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.*;
import static org.jetlinks.supports.rpc.RpcSpiTestSupport.*;

public class DefaultRpcManagerTest {
    @RpcApi("test/echo")
    public interface Echo {
        Mono<ByteBuf> bytes(ByteBuf request);
        Mono<String> context(String request);
        Mono<String> error(String request);
        Mono<String> empty(String request);
        Mono<Void> ack(String request);
        @RpcMethod(fireAndForget = true) Mono<Void> notify(String request);
        Mono<ByteBuf> pending(ByteBuf request);
        Flux<ByteBuf> stream(ByteBuf request);
        Flux<ByteBuf> channel(Flux<ByteBuf> request);
    }
    public static class EchoImpl implements Echo {
        final AtomicInteger entered = new AtomicInteger();
        final Sinks.One<Void> subscribed = Sinks.one(), cancelled = Sinks.one(), notified = Sinks.one();
        public Mono<ByteBuf> bytes(ByteBuf request) { return Mono.just(request.retainedSlice()); }
        public Mono<String> context(String request) { entered.incrementAndGet(); return Mono.deferContextual(ctx -> Mono.just(ctx.getOrDefault("user", "anonymous"))); }
        public Mono<String> error(String request) { return Mono.error(new org.hswebframework.web.exception.BusinessException.NoStackTrace(request)); }
        public Mono<String> empty(String request) { return Mono.empty(); }
        public Mono<Void> ack(String request) { return Mono.empty(); }
        public Mono<Void> notify(String request) { notified.tryEmitEmpty(); return Mono.empty(); }
        public Mono<ByteBuf> pending(ByteBuf request) {
            return Mono.<ByteBuf>never().doOnSubscribe(ignored -> subscribed.tryEmitEmpty()).doFinally(ignored -> cancelled.tryEmitEmpty());
        }
        public Flux<ByteBuf> stream(ByteBuf request) { return Flux.range(0, 32).map(i -> request.retainedSlice()).doOnDiscard(ByteBuf.class, ByteBuf::release); }
        public Flux<ByteBuf> channel(Flux<ByteBuf> request) { return request; }
    }
    static final class Fixture implements AutoCloseable {
        final Group group = new Group();
        final DefaultRpcManager first, second;
        final EchoImpl implementation = new EchoImpl();
        final Disposable registration;
        final Echo echo;
        Fixture() { this(ContextCodec.DEFAULT, ContextCodec.DEFAULT); }
        Fixture(ContextCodec firstContext, ContextCodec secondContext) {
            first = manager("a", group.discovery(), firstContext);
            second = manager("b", group.discovery(), secondContext);
            registration = second.registerService(implementation);
            first.start().block(WAIT); second.start().block(WAIT);
            echo = service(first, "b", Echo.class);
        }
        public void close() {
            first.stop().block(WAIT); second.stop().block(WAIT);
            assertEquals(0, first.activeCalls()); assertEquals(0, second.activeCalls());
        }
    }
    private static ByteBuf owner() { return Unpooled.directBuffer(8).writeLong(123); }
    private static void consume(ByteBuf buffer) { try { assertEquals(123, buffer.readLong()); } finally { buffer.release(); } }

    @Test public void registryLookupInvocationEmptyErrorAndFireAndForget() {
        try (Fixture fixture = new Fixture()) {
            assertSame(fixture.first.getService("b", Echo.class).block(WAIT), fixture.first.getService("b", Echo.class).block(WAIT));
            assertEquals(1L, fixture.first.getServices(Echo.class).count().block(WAIT).longValue());
            assertEquals(0L, fixture.first.getServices("missing", Echo.class).count().block(WAIT).longValue());
            assertNull(fixture.first.getService("missing", Echo.class).block(WAIT));
            StepVerifier.create(fixture.echo.empty("empty")).verifyComplete();
            StepVerifier.create(fixture.echo.ack("ack")).verifyComplete();
            StepVerifier.create(fixture.echo.error("remote failure")).expectErrorMatches(error -> error instanceof org.hswebframework.web.exception.BusinessException && error.getMessage().contains("remote failure")).verify(WAIT);
            fixture.echo.notify("notify").block(WAIT); fixture.implementation.notified.asMono().block(WAIT);
            ByteBuf owner = owner();
            try {
                consume(fixture.echo.bytes(owner).block(WAIT));
                assertEquals(0, owner.readerIndex());
                await(() -> owner.refCnt() == 1);
            } finally { owner.release(); }
        }
    }
    @Test public void streamAndChannelCompleteAndCancelWithAllBuffersReleased() {
        List<ByteBuf> sent = new CopyOnWriteArrayList<>();
        try (Fixture fixture = new Fixture()) {
            ByteBuf owner = owner();
            try {
                assertEquals(32L, fixture.echo.stream(owner).doOnNext(DefaultRpcManagerTest::consume).count().block(WAIT).longValue());
                assertEquals(3L, fixture.echo.stream(owner).doOnNext(DefaultRpcManagerTest::consume).take(3).count().block(WAIT).longValue());
                await(() -> owner.refCnt() == 1);
            } finally { owner.release(); }
            for (int count : new int[]{32, 3}) {
                Flux<ByteBuf> request = Flux.range(0, 32).map(i -> { ByteBuf value = owner(); sent.add(value); return value; })
                    .publishOn(reactor.core.scheduler.Schedulers.parallel(), 4);
                assertEquals(count, fixture.echo.channel(request).doOnNext(DefaultRpcManagerTest::consume).take(count).count().block(WAIT).intValue());
            }
            await(() -> sent.stream().allMatch(value -> value.refCnt() == 0));
        }
    }
    @Test public void cancellationKeepsCallerAndTransportOwnershipSeparate() {
        try (Fixture fixture = new Fixture()) {
            ByteBuf owner = owner();
            Disposable subscription = fixture.echo.pending(owner).subscribe(ByteBuf::release);
            fixture.implementation.subscribed.asMono().block(WAIT);
            owner.release(); subscription.dispose();
            fixture.implementation.cancelled.asMono().block(WAIT);
            await(() -> owner.refCnt() == 0 && fixture.first.activeCalls() == 0 && fixture.second.activeCalls() == 0);
        }
    }
    @Test public void timeoutCancelsRemoteAndReturnsItsCallLease() {
        try (Fixture fixture = new Fixture()) {
            ByteBuf owner = owner();
            try {
                StepVerifier.create(fixture.echo.pending(owner).timeout(Duration.ofMillis(500)))
                    .then(() -> fixture.implementation.subscribed.asMono().block(WAIT))
                    .expectError(TimeoutException.class).verify(WAIT);
                fixture.implementation.cancelled.asMono().block(WAIT);
                await(() -> owner.refCnt() == 1 && fixture.first.activeCalls() == 0 && fixture.second.activeCalls() == 0);
            } finally { owner.release(); }
        }
    }
    @Test public void unregisterUpdatesWatchAndPreviouslyObtainedHandleFailsExplicitly() {
        try (Fixture fixture = new Fixture()) {
            fixture.registration.dispose(); fixture.registration.dispose();
            fixture.first.watchServices(Echo.class).filter(view -> view.services().isEmpty() && view.unresolved().isEmpty()).next().block(WAIT);
            StepVerifier.create(fixture.echo.context("removed")).expectError(IllegalStateException.class).verify(WAIT);
        }
    }
    @Test public void sameLogicalNodeRestartRefreshesHandleAndIgnoresOldIdentity() {
        try (Fixture fixture = new Fixture()) {
            var old = fixture.second.localNode();
            fixture.second.stop().block(WAIT);
            DefaultRpcManager replacement = manager("b", fixture.group.discovery(), ContextCodec.DEFAULT);
            replacement.registerService(new EchoImpl());
            replacement.start().block(WAIT);
            try {
                service(fixture.first, "b", Echo.class);
                fixture.group.update(null, old.identity());
                assertEquals("anonymous", fixture.echo.context("restart").block(WAIT));
            } finally { replacement.stop().block(WAIT); }
        }
    }
    @Test public void contextIsCapturedPerSubscriptionAndReleasedAfterErrorAndCancellation() {
        CountingContext codec = new CountingContext();
        try (Fixture fixture = new Fixture(codec, codec)) {
            Mono<String> assembled = fixture.echo.context("value");
            assertEquals(0, codec.captured.get());
            assertEquals("one", assembled.contextWrite(Context.of("user", "one")).block(WAIT));
            assertEquals("two", assembled.contextWrite(Context.of("user", "two")).block(WAIT));
            StepVerifier.create(fixture.echo.error("failed").contextWrite(Context.of("user", "three")))
                .expectErrorMatches(error -> error instanceof org.hswebframework.web.exception.BusinessException && "failed".equals(error.getMessage())).verify(WAIT);
            ByteBuf owner = owner();
            try {
                var pending = fixture.echo.pending(owner).contextWrite(Context.of("user", "four")).subscribe();
                fixture.implementation.subscribed.asMono().block(WAIT); pending.dispose();
                fixture.implementation.cancelled.asMono().block(WAIT);
                await(() -> codec.captured.get() == codec.disposed.get() && owner.refCnt() == 1);
            } finally { owner.release(); }
        }
    }

    static final class CountingContext implements ContextCodec {
        final AtomicInteger captured = new AtomicInteger(), disposed = new AtomicInteger();
        record Captured(String user, Runnable cleanup) implements SerializedContext {
            public String getContext() { throw new AssertionError("Binary RPC must not request a context String"); }
            public void writeTo(ByteBuf target) {
                target.writeInt(ByteBufUtil.utf8Bytes(user));
                ByteBufUtil.writeUtf8(target, user);
            }
            public void dispose() { cleanup.run(); }
        }
        public String formatId() { return "test-user/1"; }
        public SerializedContext serialize(ContextView context) {
            captured.incrementAndGet(); return new Captured(context.getOrDefault("user", "anonymous"), disposed::incrementAndGet);
        }
        public Context deserializeFrom(Context base, ByteBuf source) {
            int size = source.readInt();
            return base.put("user", source.readCharSequence(size, java.nio.charset.StandardCharsets.UTF_8).toString());
        }
    }
}
