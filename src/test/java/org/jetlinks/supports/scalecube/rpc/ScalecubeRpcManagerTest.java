package org.jetlinks.supports.scalecube.rpc;


import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Locale;

public class ScalecubeRpcManagerTest {


    @Test
    @SneakyThrows
    public void test() {
        ExtendedCluster cluster = new ExtendedClusterImpl(
                ClusterConfig
                        .defaultConfig()
                        .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
        ).startAwait();

        ScalecubeRpcManager manager1, manager2, manager3;

        {
            manager1 = new ScalecubeRpcManager(cluster, RSocketServiceTransport::new);
            manager1.registerService(new ServiceImpl("1"));
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
            manager2.registerService(new ServiceImpl("2"));
            manager2.startAwait();

        }

        {
            ExtendedCluster cluster3 = new ExtendedClusterImpl(
                    ClusterConfig
                            .defaultConfig()
                            .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                            .membership(conf -> conf.seedMembers(cluster.address()))
            ).startAwait();
            manager3 = new ScalecubeRpcManager(cluster3, RSocketServiceTransport::new);
            manager3.startAwait();
        }

        Thread.sleep(2000);

        manager3.getServices(Service.class)
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();

        manager3.getService(cluster.member().id(), Service.class)
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


    @io.scalecube.services.annotations.Service
    public interface Service {

        @ServiceMethod
        Mono<String> upper(String value);


    }

    @AllArgsConstructor
    public static class ServiceImpl implements Service {

        private final String prefix;

        @Override
        public Mono<String> upper(String value) {
            return Mono.just(prefix +( value.toUpperCase(Locale.ROOT)));
        }

    }
}