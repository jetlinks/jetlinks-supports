package org.jetlinks.supports.scalecube.rpc;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;

public class ScalecubeRpcManagerTest {
    Member node1, node2, node3;
    ScalecubeRpcManager manager1, manager2, manager3;


    @Before
    public void init() {
        ExtendedCluster cluster = new ExtendedClusterImpl(
                ClusterConfig
                        .defaultConfig()
                        .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
        ).startAwait();
        node1 = cluster.member();

        {
            manager1 = new ScalecubeRpcManager(cluster, RSocketServiceTransport::new);
            manager1.startAwait();

        }

        {
            ExtendedCluster cluster2 = new ExtendedClusterImpl(
                    ClusterConfig
                            .defaultConfig()
                            .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                            .membership(conf -> conf.seedMembers(cluster.address()))
            ).startAwait();
            manager2 = new ScalecubeRpcManager(cluster2, RSocketServiceTransport::new);
            manager2.startAwait();
            node2 = cluster2.member();
        }

        {
            ExtendedCluster cluster3 = new ExtendedClusterImpl(
                    ClusterConfig
                            .defaultConfig()
                            .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                            .membership(conf -> conf.seedMembers(cluster.address()))
            ).startAwait();
            node3 = cluster3.member();
            manager3 = new ScalecubeRpcManager(cluster3, RSocketServiceTransport::new);
            manager3.startAwait();
        }
    }

    @After
    public void shutdown() {
        manager3.stopAwait();
        manager2.stopAwait();
        manager1.stopAwait();
    }

    @Test
    @SneakyThrows
    public void testSimple() {

        manager1.registerService(new ServiceImpl("1"));

        manager2.registerService(new ServiceImpl("2"));

        Thread.sleep(2000);

        manager3.getServices(Service.class)
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();

        manager3.getService(node1.id(), Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("1TEST")
                .verifyComplete();

        manager2.stopAwait();
        manager3.getServices(Service.class)
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

    }

    @Test
    @SneakyThrows
    public void testCustomId() {
        manager1.registerService("s1", new ServiceImpl("1"));
        manager1.registerService("s2", new ServiceImpl("2"));
        manager2.registerService("s3", new ServiceImpl("3"));
        manager2.registerService("s1", new ServiceImpl("2-1"));

        Thread.sleep(2000);

        manager3.getServices("s1", Service.class)
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();


        manager3.getServices(Service.class)
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();

        manager3.getService(node1.id(), "s1", Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("1TEST")
                .verifyComplete();

        manager3.getService(node1.id(), "s2", Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("2TEST")
                .verifyComplete();

    }

    @Test
    public void testEvent() {

        manager3
                .listen(Service.class)
                .doOnSubscribe(s -> {
                    Mono.delay(Duration.ofSeconds(1))
                        .subscribe(ignore -> {
                            Disposable disposable = manager1
                                    .registerService("t1", new ServiceImpl("t1"));
                            Mono.delay(Duration.ofSeconds(1))
                                .subscribe(i -> {
                                    disposable.dispose();
                                });
                        });

                })
                .take(2)
                .timeout(Duration.ofSeconds(4))
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();

    }

    @Test
    public void testRegisterTime() {

        manager3
                .listen(Service.class)
                .doOnSubscribe(s -> {
                    Mono.delay(Duration.ofSeconds(1))
                        .subscribe(ignore -> {
                            Disposable disposable = manager1
                                    .registerService("t1", new ServiceImpl("t1"));
                            Mono.delay(Duration.ofSeconds(1))
                                .subscribe(i -> {
                                    disposable.dispose();
                                });
                        });

                })
                .take(2)
                .timeout(Duration.ofSeconds(4))
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();

    }

    @Test
    @SneakyThrows
    public void testNative() {
        manager1.registerService("n1", new ServiceImpl("1"));

        Thread.sleep(2000);

        manager3.getService(node1.id(), "n1", Service.class)
                .flatMapMany(service -> service.read("test"))
                .map(buf -> buf.toString(StandardCharsets.UTF_8))
                .as(StepVerifier::create)
                .expectNext("hel", "lo")
                .verifyComplete();

        manager3.getService(node1.id(), "n1", Service.class)
                .flatMapMany(service -> service.read0(Unpooled.wrappedBuffer("test".getBytes())))
                .map(buf -> buf.toString(StandardCharsets.UTF_8))
                .as(StepVerifier::create)
                .expectNext("test","hel", "lo")
                .verifyComplete();
    }

    @io.scalecube.services.annotations.Service
    public interface Service {

        @ServiceMethod
        Mono<String> upper(String value);

        @ServiceMethod
        Flux<ByteBuf> read(String id);

        @ServiceMethod
        Flux<ByteBuf> read0(ByteBuf buf);

    }

    @AllArgsConstructor
    public static class ServiceImpl implements Service {

        private final String prefix;

        @Override
        public Mono<String> upper(String value) {
            return Mono.just(prefix + (value.toUpperCase(Locale.ROOT)));
        }

        @Override
        public Flux<ByteBuf> read0(ByteBuf buf) {
            return Flux.just(buf,
                             Unpooled.wrappedBuffer("hel".getBytes()),
                             Unpooled.wrappedBuffer("lo".getBytes()));
        }

        @Override
        public Flux<ByteBuf> read(String id) {
//            return Flux.just("1","2");
            return Flux.just(Unpooled.wrappedBuffer("hel".getBytes()),
                             Unpooled.wrappedBuffer("lo".getBytes()));
        }
    }
}