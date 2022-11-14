package org.jetlinks.supports.scalecube.event;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.property.ReportPropertyMessage;
import org.jetlinks.supports.event.BrokerEventBus;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.jetlinks.supports.scalecube.rpc.ScalecubeRpcManager;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class ClusterEventBusBrokerTest {


    @Test
    @SneakyThrows
    public void test() {

        ExtendedClusterImpl cluster = new ExtendedClusterImpl(new ClusterConfig()
                                                                      .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                                                                      .memberAlias("cluster1")
        );
        cluster.startAwait();

        ExtendedClusterImpl cluster2 = new ExtendedClusterImpl(new ClusterConfig()
                                                                       .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                                                                       .membership(ship -> ship.seedMembers(cluster.address()))
                                                                       .memberAlias("cluster2")
        );
        cluster2.startAwait();

        BrokerEventBus eventBus1 = new BrokerEventBus();
        BrokerEventBus eventBus2 = new BrokerEventBus();

        {
            ScalecubeRpcManager rpcManager = new ScalecubeRpcManager()
                    .cluster(cluster)
                    .transport(RSocketServiceTransport::new);
            rpcManager.startAwait();
            eventBus1.addBroker(new ClusterEventBusBroker(rpcManager));
        }

        {
            ScalecubeRpcManager rpcManager = new ScalecubeRpcManager()
                    .cluster(cluster2)
                    .transport(RSocketServiceTransport::new);
            rpcManager.startAwait();
            eventBus2.addBroker(new ClusterEventBusBroker(rpcManager));
        }

        Thread.sleep(1000);
        eventBus2.subscribe(Subscription
                                    .builder()
                                    .topics("/test")
                                    .justBroker()
                                    .subscriberId("test")
                                    .build())
                 .doOnSubscribe(sub -> {
                     Mono.delay(Duration.ofMillis(200))
                         .thenMany(Flux.range(0, 100))
                         .flatMap(i -> eventBus1.publish("/test", i))
                         .subscribe();
                 })
                 .take(100)
                 .timeout(Duration.ofSeconds(3))
                 .as(StepVerifier::create)
                 .expectNextCount(100)
                 .verifyComplete();

        Flux.range(0, 101_00000)
            .flatMap(i -> eventBus1.publish("/test", new ReportPropertyMessage()))
            .subscribe();

        Duration time = eventBus2
                .subscribe(Subscription
                                   .builder()
                                   .topics("/test")
                                   .justBroker()
                                   .subscriberId("test")
                                   .build())
                .take(100_0000)
                .as(StepVerifier::create)
                .expectNextCount(100_0000)
                .verifyComplete();
        System.out.println(time);

    }
}