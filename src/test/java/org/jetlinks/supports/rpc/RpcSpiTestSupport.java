package org.jetlinks.supports.rpc;

import org.jetlinks.core.cluster.discovery.*;
import org.jetlinks.core.rpc.*;
import org.jetlinks.supports.rpc.rsocket.RSocketRpcTransport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.net.URI;
import io.rsocket.transport.netty.server.CloseableChannel;
import java.time.Duration;
import java.util.*;
import java.util.function.BooleanSupplier;

final class RpcSpiTestSupport {
    static final Duration WAIT = Duration.ofSeconds(10);
    static URI address(CloseableChannel server) {
        return URI.create("tcp://" + server.address().getHostString() + ":" + server.address().getPort());
    }
    static NodeAnnouncement node(String id) {
        return new NodeAnnouncement(new NodeIdentity(id, UUID.randomUUID()), "test", "test", Map.of(), Map.of());
    }
    static void await(BooleanSupplier condition) {
        Mono.fromSupplier(() -> condition.getAsBoolean() ? Boolean.TRUE : null)
            .repeatWhenEmpty(repeat -> repeat.delayElements(Duration.ofMillis(2))).block(WAIT);
    }
    static void expectFailure(Class<? extends Throwable> type, Runnable action) {
        try { action.run(); }
        catch (Throwable error) {
            if (type.isInstance(error)) return;
            throw new AssertionError("Expected " + type.getName(), error);
        }
        throw new AssertionError("Expected " + type.getName());
    }
    static DefaultRpcManager manager(String id, NodeDiscovery discovery, ContextCodec context) {
        return new DefaultRpcManager(node(id), discovery, new RSocketRpcTransport(2),
            new JavaRpcBodyCodec(), context, RpcOptions.defaults(URI.create("tcp://127.0.0.1:0")));
    }
    static <T> T service(DefaultRpcManager from, String nodeId, Class<T> api) {
        from.watchServices(api).filter(view -> view.unresolved().isEmpty() && view.services().stream().anyMatch(service -> service.serverNodeId().equals(nodeId)))
            .next().block(WAIT);
        return from.getService(nodeId, api).block(WAIT);
    }

    static final class Group {
        final Map<NodeIdentity, NodeAnnouncement> nodes = new HashMap<>();
        final LatestValue<ClusterView> view = new LatestValue<>(new ClusterView(0, Map.of()));
        synchronized void update(NodeAnnouncement add, NodeIdentity remove) {
            if (add != null) nodes.put(add.identity(), add);
            if (remove != null) nodes.remove(remove);
            view.update(new ClusterView(view.current().revision() + 1, nodes));
        }
        NodeDiscovery discovery() {
            return new NodeDiscovery() {
                NodeAnnouncement local;
                public Mono<Void> start(NodeAnnouncement node) { return Mono.fromRunnable(() -> { local = node; update(node, null); }); }
                public ClusterView currentView() { return view.current(); }
                public Flux<ClusterView> watch() { return view.watch(); }
                public Mono<Void> stop() { return Mono.fromRunnable(() -> { if (local != null) update(null, local.identity()); }); }
            };
        }
    }
}
