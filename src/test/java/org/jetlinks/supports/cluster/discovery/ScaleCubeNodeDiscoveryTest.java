package org.jetlinks.supports.cluster.discovery;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import org.jetlinks.core.cluster.discovery.*;
import org.jetlinks.core.rpc.ContextCodec;
import org.jetlinks.core.rpc.annotation.RpcApi;
import org.jetlinks.supports.rpc.*;
import org.jetlinks.supports.rpc.rsocket.RSocketRpcTransport;
import org.junit.Test;
import reactor.core.publisher.Mono;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import static org.junit.Assert.*;

public class ScaleCubeNodeDiscoveryTest {
    private static final Duration WAIT = Duration.ofSeconds(15);
    private static ClusterConfig config(String namespace, URI seed) {
        return new ClusterConfig().transport(transport -> transport.port(0).transportFactory(new TcpTransportFactory()))
            .membership(membership -> membership.namespace(namespace).seedMembers(seed == null ? List.of() : List.of(Address.create(seed.getHost(), seed.getPort()))));
    }
    private static NodeAnnouncement node(String id) {
        return new NodeAnnouncement(new NodeIdentity(id, UUID.randomUUID()), "test", "test", Map.of(), Map.of());
    }
    @Test public void joinLeaveAndSameLogicalNodeRestartPublishFreshIdentity() {
        String namespace = "spi-" + UUID.randomUUID();
        ScaleCubeNodeDiscovery first = new ScaleCubeNodeDiscovery(config(namespace, null));
        first.start(node("a")).block(WAIT);
        ScaleCubeNodeDiscovery second = new ScaleCubeNodeDiscovery(config(namespace, first.address()));
        NodeAnnouncement old = node("b");
        second.start(old).block(WAIT);
        try {
            first.watch().filter(view -> view.members().containsKey(old.identity())).next().block(WAIT);
            second.stop().block(WAIT);
            first.watch().filter(view -> view.members().size() == 1).next().block(WAIT);
            ScaleCubeNodeDiscovery replacement = new ScaleCubeNodeDiscovery(config(namespace, first.address()));
            NodeAnnouncement fresh = node("b");
            replacement.start(fresh).block(WAIT);
            try {
                ClusterView joined = first.watch().filter(view -> view.members().containsKey(fresh.identity())).next().block(WAIT);
                assertFalse(joined.members().containsKey(old.identity()));
            } finally { replacement.stop().block(WAIT); }
        } finally { second.stop().block(WAIT); first.stop().block(WAIT); }
    }

    @RpcApi("discovery/echo") public interface Echo { Mono<String> echo(String value); }
    public static class EchoImpl implements Echo { public Mono<String> echo(String value) { return Mono.just(value); } }

    @Test public void nativeRpcUsesActualMembershipWithoutScaleCubeServiceDiscovery() {
        String namespace = "rpc-spi-" + UUID.randomUUID();
        ScaleCubeNodeDiscovery firstDiscovery = new ScaleCubeNodeDiscovery(config(namespace, null));
        DefaultRpcManager first = manager("a", firstDiscovery);
        first.start().block(WAIT);
        DefaultRpcManager second = manager("b", new ScaleCubeNodeDiscovery(config(namespace, firstDiscovery.address())));
        second.registerService(new EchoImpl());
        second.start().block(WAIT);
        try {
            first.watchServices(Echo.class).filter(view -> view.unresolved().isEmpty() && view.services().size() == 1).next().block(WAIT);
            assertEquals("hello", first.getService("b", Echo.class).flatMap(service -> service.echo("hello")).block(WAIT));
        } finally { first.stop().block(WAIT); second.stop().block(WAIT); }
    }
    private static DefaultRpcManager manager(String id, NodeDiscovery discovery) {
        return new DefaultRpcManager(node(id), discovery, new RSocketRpcTransport(2), new JavaRpcBodyCodec(), ContextCodec.DEFAULT,
            RpcOptions.defaults(URI.create("tcp://127.0.0.1:0")));
    }
}
