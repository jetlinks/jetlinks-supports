package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.jetlinks.core.cluster.discovery.*;
import org.jetlinks.core.rpc.*;
import org.jetlinks.core.rpc.transport.*;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.core.trace.FluxTracer;
import org.reactivestreams.Publisher;
import reactor.core.*;
import reactor.core.publisher.*;
import reactor.core.scheduler.*;
import reactor.util.context.*;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.*;
import java.util.stream.Collector;
import java.util.function.Function;

/** 组合发现、服务目录及传输。控制面串行，业务调用只读本地 peer 并直接执行。 */
public final class DefaultRpcManager implements RpcManager {
    private volatile NodeAnnouncement local;
    private final NodeDiscovery discovery;
    private final RpcTransport transport;
    private final RpcBodyCodec bodyCodec;
    private final ContextCodec contextCodec;
    private final RpcOptions options;
    private final DefaultRpcServiceRegistry registry;
    private final Scheduler control = Schedulers.newSingle("rpc-control", true);
    private final Disposable.Composite lifecycle = Disposables.composite();
    private final Map<NodeIdentity, Peer> peers = new ConcurrentHashMap<>();
    private volatile Map<String, Peer> routes = Map.of();
    private volatile Set<String> conflicts = Set.of();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicBoolean started = new AtomicBoolean(), stopped = new AtomicBoolean(), catalogScheduled = new AtomicBoolean();
    private RpcServer server;

    public DefaultRpcManager(NodeAnnouncement local, NodeDiscovery discovery, RpcTransport transport,
                             RpcBodyCodec bodyCodec, ContextCodec contextCodec, RpcOptions options) {
        this.local = local;
        this.discovery = discovery;
        this.transport = transport;
        this.bodyCodec = bodyCodec;
        this.contextCodec = contextCodec;
        this.options = options;
        registry = new DefaultRpcServiceRegistry(local.identity(), this::bind, this::localChanged);
    }

    public NodeAnnouncement localNode() { return local; }
    public int activeCalls() { return active.get(); }
    public String currentServerId() { return local.identity().nodeId(); }
    public boolean isShutdown() { return stopped.get(); }

    public Mono<Void> start() {
        return Mono.defer(() -> {
            if (stopped.get() || !started.compareAndSet(false, true)) return Mono.error(new IllegalStateException("RPC already started/stopped"));
            return transport.bind(options.listen(), options.maxPayloadBytes(), this::accept).doOnNext(bound -> {
                server = bound;
                Map<String, URI> endpoints = new HashMap<>(local.endpoints());
                endpoints.putIfAbsent("rpc", bound.address());
                local = new NodeAnnouncement(local.identity(), local.namespace(), local.application(), local.labels(), endpoints);
            }).then(Mono.defer(() -> discovery.start(local))).doOnSuccess(ignored -> lifecycle.add(discovery.watch()
                .publishOn(control, 1).subscribe(this::reconcile, this::discoveryFailed)))
                .onErrorResume(error -> stop().then(Mono.error(error)));
        });
    }

    private void executeControl(Runnable action) { if (!stopped.get() && !registry.isFailed()) control.schedule(() -> { if (!stopped.get() && !registry.isFailed()) action.run(); }); }

    private void reconcile(ClusterView view) {
        if (stopped.get() || registry.isFailed()) return;
        Set<NodeIdentity> desired = new HashSet<>();
        view.members().values().stream().filter(node -> local.namespace().equals(node.namespace()) && node.endpoints().containsKey("rpc"))
            .forEach(node -> desired.add(node.identity()));
        desired.remove(local.identity());
        for (Peer peer : peers.values()) {
            NodeAnnouncement announced = view.members().get(peer.node.identity());
            if (announced == null || !announced.endpoints().equals(peer.node.endpoints())) retire(peer);
        }
        for (NodeIdentity identity : desired) {
            NodeAnnouncement node = view.members().get(identity);
            Peer peer = peers.computeIfAbsent(identity, ignored -> new Peer(node));
            if (compare(local.identity(), identity) < 0) connect(peer);
        }
        publishRoutes();
        registry.members(desired);
    }
    private static int compare(NodeIdentity left, NodeIdentity right) {
        int result = left.nodeId().compareTo(right.nodeId());
        return result == 0 ? left.incarnation().compareTo(right.incarnation()) : result;
    }

    private Mono<RpcHandler> accept(RpcFrame frame, RpcConnection reverse) {
        return Mono.fromCallable(() -> {
            registry.checkAvailable();
            checkFrameBudget(frame);
            RpcProtocol.Setup setup = RpcProtocol.readSetup(frame);
            ClusterView view = discovery.currentView();
            NodeAnnouncement node = view.members().get(setup.identity());
            if (node == null || !local.namespace().equals(setup.namespace()) || !local.namespace().equals(node.namespace()) || !options.token().equals(setup.token())
                || !bodyCodec.id().equals(setup.bodyCodec()) || !contextCodec.formatId().equals(setup.contextCodec())
                || compare(local.identity(), node.identity()) <= 0) {
                throw new IllegalArgumentException("RPC peer setup rejected");
            }
            reconcile(view);
            Peer peer = peers.computeIfAbsent(node.identity(), ignored -> new Peer(node));
            if (peer.connection != null) throw new IllegalStateException("Duplicate RPC peer connection");
            peer.connection = reverse;
            watchClose(peer);
            return handler(peer);
        }).subscribeOn(control);
    }

    private void connect(Peer peer) {
        if (stopped.get() || registry.isFailed() || peer.retired || peer.connection != null || peer.connecting) return;
        peer.connecting = true;
        peer.work.update(transport.connect(peer.node.endpoints().get("rpc"), options.maxPayloadBytes(),
                () -> RpcProtocol.setup(new RpcProtocol.Setup(local.identity(), local.namespace(), bodyCodec.id(), contextCodec.formatId(), options.token())), handler(peer))
            .timeout(options.connectTimeout()).publishOn(control).subscribe(connection -> {
                peer.connecting = false;
                if (stopped.get() || peer.retired) { connection.close().subscribe(); return; }
                peer.connection = connection;
                watchClose(peer);
                sync(peer);
            }, error -> disconnected(peer)));
    }
    private void watchClose(Peer peer) {
        peer.closeWatcher.update(peer.connection.onClose().subscribe(ignored -> {},
            error -> executeControl(() -> disconnected(peer)), () -> executeControl(() -> disconnected(peer))));
    }
    private void disconnected(Peer peer) {
        if (peer.retired || stopped.get()) return;
        retire(peer);
        NodeAnnouncement announced = discovery.currentView().members().get(peer.node.identity());
        if (announced != null) {
            Peer replacement = peers.computeIfAbsent(announced.identity(), ignored -> new Peer(announced));
            if (compare(local.identity(), announced.identity()) < 0) {
                replacement.work.update(control.schedule(() -> connect(replacement), options.reconnectDelay().toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS));
            }
        }
    }
    private void retire(Peer peer) {
        if (!peers.remove(peer.node.identity(), peer)) return;
        peer.retired = true;
        peer.work.dispose(); peer.sync.dispose(); peer.closeWatcher.dispose();
        publishRoutes();
        registry.unavailable(peer.node.identity());
        if (peer.connection != null) peer.connection.close().subscribe();
    }

    private void localChanged() {
        if (!started.get() || stopped.get() || !catalogScheduled.compareAndSet(false, true)) return;
        executeControl(() -> {
            catalogScheduled.set(false);
            for (Peer peer : peers.values()) { peer.dirty = true; sync(peer); }
        });
    }
    private void sync(Peer peer) {
        if (peer.retired || peer.connection == null || peer.syncing || stopped.get()) return;
        peer.syncing = true; peer.dirty = false;
        peer.sync.update(peer.connection.requestResponse(() -> RpcProtocol.catalog(local.identity(), registry.localCatalog(), true))
            .timeout(options.connectTimeout()).publishOn(control).doOnDiscard(RpcFrame.class, RpcFrame::release)
            .subscribe(frame -> {
                try { checkFrameBudget(frame); RpcProtocol.checkResponse(frame); install(peer, RpcProtocol.readCatalog(frame, peer.node.identity())); }
                finally { frame.release(); }
            }, error -> disconnected(peer), () -> {
                peer.syncing = false;
                if (peer.dirty) sync(peer);
            }));
    }
    private void install(Peer peer, DefaultRpcServiceRegistry.Catalog catalog) {
        if (peer.retired || peer.connection == null || peers.get(peer.node.identity()) != peer || catalog.revision() <= peer.remoteRevision) return;
        peer.remoteRevision = catalog.revision();
        peer.catalog = catalog.services();
        publishRoutes();
        registry.ready(peer.node.identity(), catalog.services());
    }
    private void discoveryFailed(Throwable error) {
        if (stopped.get()) return;
        registry.fail(error);
        for (Peer peer : List.copyOf(peers.values())) retire(peer);
    }

    private RpcHandler handler(Peer peer) {
        var unary = new RpcServerMono.Plan() {
            AtomicInteger acquire() { return slot(); }
            Mono<?> invoke(RpcFrame request, RpcServerMono.CallSubscriber subscriber) {
                RpcProtocol.Call call = readCall(request);
                var method = registry.localMethod(call.route());
                requireMode(method.definition(), false, false);
                subscriber.context = contextCodec.deserializeFrom(subscriber.currentContext(), call.context());
                // Keep method entry inside its tracing boundary after restoring security context.
                return Mono.defer(() -> Mono.from(invokeMethod(method, decodeRequest(request, method.definition(), false))))
                    .as(MonoTracer.create(method.definition().serverSpan));
            }
            RpcFrame encode(Object value) { return encodeResponse(value); }
        };
        return new RpcHandler() {
            public Mono<RpcFrame> requestResponse(RpcFrame request) {
                if (RpcProtocol.peekKind(request.metadata()) == RpcProtocol.CATALOG) {
                    checkFrameBudget(request);
                    return Mono.fromCallable(() -> {
                        install(peer, RpcProtocol.readCatalog(request, peer.node.identity()));
                        return RpcProtocol.catalog(local.identity(), registry.localCatalog(), false);
                    }).subscribeOn(control).onErrorResume(error -> Mono.just(RpcProtocol.error(error)));
                }
                return RpcServerMono.create(unary, request);
            }
            public Flux<RpcFrame> requestStream(RpcFrame request) {
                return Flux.using(DefaultRpcManager.this::slot, ignored -> {
                    RpcProtocol.Call call = readCall(request);
                    var method = registry.localMethod(call.route());
                    requireMode(method.definition(), true, false);
                    return invokeMany(method, () -> decodeRequest(request, method.definition(), false), call.context())
                        .map(DefaultRpcManager.this::encodeResponse);
                }, AtomicInteger::decrementAndGet, false).onErrorResume(error -> Flux.just(RpcProtocol.error(error)));
            }
            public Flux<RpcFrame> requestChannel(Publisher<RpcFrame> requests) {
                return Flux.using(DefaultRpcManager.this::slot, ignored -> Flux.from(requests)
                    .switchOnFirst((signal, frames) -> {
                        if (!signal.hasValue()) return frames;
                        RpcProtocol.Call call = readCall(signal.get());
                        var method = registry.localMethod(call.route());
                        requireMode(method.definition(), true, true);
                        Flux<Object> values = frames.map(frame -> {
                            try { return decodeRequest(frame, method.definition(), true); }
                            finally { frame.release(); }
                        }).doOnDiscard(ByteBuf.class, ByteBuf::release);
                        return invokeMany(method, () -> method.definition().requestType == Mono.class ? values.next() : values, call.context())
                            .map(DefaultRpcManager.this::encodeResponse);
                    }).doOnDiscard(RpcFrame.class, RpcFrame::release), AtomicInteger::decrementAndGet, false)
                    .onErrorResume(error -> Flux.just(RpcProtocol.error(error)));
            }
            public Mono<Void> fireAndForget(RpcFrame request) {
                return Mono.using(DefaultRpcManager.this::slot, ignored -> {
                    RpcProtocol.Call call = readCall(request);
                    var method = registry.localMethod(call.route());
                    if (!method.definition().forget) return Mono.error(new IllegalArgumentException("RPC method requires acknowledgement"));
                    return invokeOne(method, request, call.context()).then();
                }, AtomicInteger::decrementAndGet, false);
            }
        };
    }

    private static void requireMode(RpcMethodBinding method, boolean stream, boolean channel) {
        if (method.stream != stream || method.channel != channel || method.forget) throw new IllegalArgumentException("RPC interaction mismatch");
    }
    private void checkFrameBudget(RpcFrame frame) {
        if ((long) frame.data().readableBytes() + frame.metadata().readableBytes() > options.maxPayloadBytes()) {
            throw new IllegalArgumentException("RPC frame too large");
        }
    }
    private RpcProtocol.Call readCall(RpcFrame frame) {
        checkFrameBudget(frame);
        return RpcProtocol.readCall(frame.metadata());
    }
    private Mono<?> invokeOne(DefaultRpcServiceRegistry.BoundMethod method, RpcFrame frame, ByteBuf context) {
        if (contextCodec == ContextCodec.DEFAULT) {
            contextCodec.deserializeFrom(Context.empty(), context);
            return Mono.from(invokeMethod(method, decodeRequest(frame, method.definition(), false)))
                .as(MonoTracer.create(method.definition().serverSpan));
        }
        // Restore security context before decoding the body or entering even the synchronous API method.
        return Mono.defer(() -> Mono.from(invokeMethod(method, decodeRequest(frame, method.definition(), false))))
            .as(MonoTracer.create(method.definition().serverSpan))
            .contextWrite(base -> contextCodec.deserializeFrom(base, context));
    }
    private Flux<?> invokeMany(DefaultRpcServiceRegistry.BoundMethod method, java.util.function.Supplier<Object> request, ByteBuf context) {
        if (contextCodec == ContextCodec.DEFAULT) {
            contextCodec.deserializeFrom(Context.empty(), context);
            return Flux.from(invokeMethod(method, request.get())).as(FluxTracer.create(method.definition().serverSpan));
        }
        return Flux.defer(() -> Flux.from(invokeMethod(method, request.get())))
            .as(FluxTracer.create(method.definition().serverSpan))
            .contextWrite(base -> contextCodec.deserializeFrom(base, context));
    }
    private static Publisher<?> invokeMethod(DefaultRpcServiceRegistry.BoundMethod method, Object request) {
        try { return method.invoke(request); }
        catch (Throwable error) { return Mono.error(error); }
    }

    // Each using owns one increment; the shared counter avoids a per-call cleanup callback.
    private AtomicInteger slot() {
        if (stopped.get()) throw new IllegalStateException("RPC stopped");
        if (active.incrementAndGet() > options.maxInFlight()) {
            active.decrementAndGet();
            throw new RejectedExecutionException("RPC in-flight limit reached");
        }
        return active;
    }

    private void publishRoutes() {
        Map<String, Peer> ready = new HashMap<>();
        Set<String> seen = new HashSet<>(), duplicate = new HashSet<>();
        seen.add(currentServerId());
        for (Peer peer : peers.values()) {
            String id = peer.node.identity().nodeId();
            if (!seen.add(id)) duplicate.add(id);
            if (peer.connection != null && peer.remoteRevision >= 0) ready.put(id, peer);
        }
        duplicate.forEach(ready::remove);
        conflicts = Set.copyOf(duplicate);
        routes = Map.copyOf(ready);
    }
    private RpcConnection target(String nodeId, DefaultRpcServiceRegistry.ServiceKey service) {
        registry.checkAvailable();
        if (conflicts.contains(nodeId)) throw new IllegalStateException("Conflicting RPC node instances: " + nodeId);
        Peer peer = routes.get(nodeId);
        if (peer == null || !peer.catalog.contains(service)) throw new IllegalStateException("RPC service unavailable: " + nodeId + "/" + service);
        return peer.connection;
    }

    private Function<Object, Object> bind(String nodeId, DefaultRpcServiceRegistry.ServiceKey key, RpcMethodBinding method, byte[] route) {
        boolean localCall = nodeId.equals(currentServerId());
        if (method.channel) {
            return request -> Flux.deferContextual(context -> Flux.<Object, CallLease>using(() -> lease(context, null, route, localCall), lease -> {
                if (localCall) return Flux.from(invokeMethod(registry.localMethod(key, method.name), request));
                Flux<RpcFrame> frames = Flux.from((Publisher<?>) request).index((index, value) -> channelFrame(lease, index == 0, value))
                    .doOnDiscard(ByteBuf.class, ByteBuf::release);
                return target(nodeId, key).requestChannel(frames).<Object>handle((frame, sink) -> deliver(frame, method.resultType, sink));
            }, CallLease::dispose, false)).as(FluxTracer.create(method.clientSpan));
        }
        if (method.stream) {
            return request -> Flux.deferContextual(context -> Flux.<Object, CallLease>using(() -> lease(context, request, route, localCall), lease -> {
                if (localCall) return Flux.from(invokeMethod(registry.localMethod(key, method.name), lease.localRequest(request)));
                return target(nodeId, key).requestStream(lease.frame::retainedDuplicate).<Object>handle((frame, sink) -> deliver(frame, method.resultType, sink));
            }, CallLease::dispose, false)).as(FluxTracer.create(method.clientSpan));
        }
        var requestTimeout = localCall || method.forget ? null : options.requestTimeout();
        var plan = new RpcCallMono.Plan<CallLease, Object>(requestTimeout,
            requestTimeout == null ? null : Schedulers.parallel()) {
            CallLease acquire(Context context, Object request) { return lease(context, request, route, localCall); }
            Mono<?> invoke(CallLease lease, Object request) {
                if (localCall) return Mono.from(invokeMethod(registry.localMethod(key, method.name), lease.localRequest(request)));
                RpcConnection connection = target(nodeId, key);
                return method.forget ? connection.fireAndForget(lease.frame::retainedDuplicate)
                    : connection.requestResponse(lease.frame::retainedDuplicate);
            }
            Object decode(Object value) {
                if (localCall || method.forget) return value;
                RpcFrame frame = (RpcFrame) value;
                try { return decodeResponse(frame, method.resultType); }
                finally { frame.release(); }
            }
        };
        return request -> RpcCallMono.create(plan, request).as(MonoTracer.create(method.clientSpan));
    }

    private CallLease lease(ContextView context, Object request, byte[] route, boolean localCall) {
        if (localCall && conflicts.contains(currentServerId())) throw new IllegalStateException("Conflicting local RPC node identity");
        AtomicInteger slot = slot();
        ByteBuf data = Unpooled.EMPTY_BUFFER, metadata = Unpooled.EMPTY_BUFFER;
        SerializedContext captured = SerializedContext.empty();
        try {
            if (request instanceof ByteBuf buffer) data = buffer.retainedSlice();
            else if (!localCall) data = encodeBody(request);
            if (!localCall) {
                captured = contextCodec.serialize(context);
                metadata = ByteBufAllocator.DEFAULT.buffer(Math.min(256, options.maxPayloadBytes()), options.maxPayloadBytes()).writeBytes(route);
                captured.writeTo(metadata);
            }
            if ((long) data.readableBytes() + metadata.readableBytes() > options.maxPayloadBytes()) throw new IllegalArgumentException("RPC frame too large");
            return new CallLease(new RpcFrame(data, metadata), captured, slot);
        } catch (Throwable error) {
            try { data.release(); metadata.release(); }
            finally { try { captured.dispose(); } finally { slot.decrementAndGet(); } }
            throw Exceptions.propagate(error);
        }
    }

    private record CallLease(RpcFrame frame, Disposable context, AtomicInteger slot) implements Disposable {
        Object localRequest(Object original) { return original instanceof ByteBuf ? frame.data().duplicate() : original; }
        public void dispose() {
            try { frame.release(); }
            finally { try { context.dispose(); } finally { slot.decrementAndGet(); } }
        }
    }

    private RpcFrame channelFrame(CallLease lease, boolean first, Object value) {
        ByteBuf data = encodeBody(value);
        try {
            if (!first) return new RpcFrame(data, Unpooled.EMPTY_BUFFER);
            // Retain the frame atomically before reading metadata: cancellation may already own cleanup.
            RpcFrame header = lease.frame.retainedDuplicate();
            try { return new RpcFrame(data, header.metadata().retainedDuplicate()); }
            finally { header.release(); }
        } catch (Throwable error) { data.release(); throw error; }
    }

    private ByteBuf encodeBody(Object value) {
        if (value == null) return Unpooled.EMPTY_BUFFER;
        if (value instanceof ByteBuf buffer) {
            if (buffer.readableBytes() > options.maxPayloadBytes()) { buffer.release(); throw new IllegalArgumentException("RPC payload too large"); }
            return buffer;
        }
        ByteBuf output = ByteBufAllocator.DEFAULT.buffer(Math.min(256, options.maxPayloadBytes()), options.maxPayloadBytes());
        try { bodyCodec.encode(value, output); return output; }
        catch (Throwable error) { output.release(); throw error; }
    }
    private RpcFrame encodeResponse(Object value) { return new RpcFrame(encodeBody(value), Unpooled.EMPTY_BUFFER); }

    private Object decodeRequest(RpcFrame frame, RpcMethodBinding method, boolean owned) {
        if (owned) checkFrameBudget(frame);
        if (method.requestDataType == Void.TYPE) return null;
        ByteBuf data = frame.data();
        if (method.requestDataType == ByteBuf.class) return owned ? data.retainedSlice() : data.duplicate();
        return data.isReadable() ? bodyCodec.decode(data.duplicate(), method.requestDataType) : null;
    }
    private void deliver(RpcFrame frame, Type type, SynchronousSink<Object> sink) {
        try {
            Object value = decodeResponse(frame, type);
            if (value != null) sink.next(value);
        } finally { frame.release(); }
    }
    private Object decodeResponse(RpcFrame frame, Type type) {
        checkFrameBudget(frame);
        RpcProtocol.checkResponse(frame);
        ByteBuf data = frame.data();
        return type == Void.class ? null : type == ByteBuf.class ? data.retainedSlice()
            : data.isReadable() ? bodyCodec.decode(data.duplicate(), type) : null;
    }

    public Mono<Void> stop() {
        return Mono.defer(() -> {
            if (!stopped.compareAndSet(false, true)) return Mono.empty();
            lifecycle.dispose();
            for (Peer peer : List.copyOf(peers.values())) retire(peer);
            registry.close();
            return Flux.concatDelayError(server == null ? Mono.<Void>empty() : server.close(), discovery.stop(), transport.close()).then()
                .doFinally(ignored -> control.dispose());
        });
    }

    private static final class Peer {
        final NodeAnnouncement node;
        volatile RpcConnection connection;
        volatile long remoteRevision = -1;
        volatile Set<DefaultRpcServiceRegistry.ServiceKey> catalog = Set.of();
        final Disposable.Swap work = Disposables.swap(), closeWatcher = Disposables.swap();
        Disposable.Swap sync = Disposables.swap();
        boolean connecting, syncing, dirty, retired;
        Peer(NodeAnnouncement node) { this.node = node; }
    }

    public <T> Disposable registerService(T service) { return registry.registerService(service); }
    public <T> Disposable registerService(String id, T service) { return registry.registerService(id, service); }
    public <I> Flux<RpcService<I>> getServices(Class<I> api) { return registry.getServices(api); }
    public <I> Flux<RpcService<I>> getServices(String id, Class<I> api) { return registry.getServices(id, api); }
    public Flux<RpcService<?>> getServices() { return registry.getServices(); }
    public <I> Mono<I> getService(String nodeId, Class<I> api) { return registry.getService(nodeId, api); }
    public <I> Mono<I> getService(String nodeId, String id, Class<I> api) { return registry.getService(nodeId, id, api); }
    public <I> Mono<RpcService<I>> selectService(Class<I> api) { return registry.selectService(api); }
    public <I> Mono<RpcService<I>> selectService(Class<I> api, Object key) { return registry.selectService(api, key); }
    public <T, I> Mono<I> selectService(Class<I> api, Collector<RpcService<I>, T, Optional<RpcService<I>>> selector, Mono<I> fallback) {
        return registry.selectService(api, selector, fallback);
    }
    public <I> Flux<ServiceEvent> listen(Class<I> api) { return registry.listen(api); }
    public <I> Flux<RpcServiceView<I>> watchServices(Class<I> api) { return registry.watchServices(api); }
}
