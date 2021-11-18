package org.jetlinks.supports.scalecube.event;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.supports.event.BrokerEventBus;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class ScalecubeEventBusBrokerTest {

    BrokerEventBus eventBus1, eventBus2, eventBus3;

    @Before
    public void init() {
        ScalecubeEventBusBroker broker1, broker2, broker3;
        Address address1, address2, address3;
        {
            ExtendedClusterImpl cluster = new ExtendedClusterImpl(new ClusterConfig()
                                                                          .memberAlias("broker1")
                                                                          .transport(cfg -> cfg.transportFactory(new TcpTransportFactory()))
            );
            cluster.startAwait();
            address1 = cluster.address();
            broker1 = new ScalecubeEventBusBroker(cluster);
            eventBus1 = new BrokerEventBus();
            eventBus1.addBroker(broker1);
        }

        Address seed = address1;
        {
            ExtendedClusterImpl cluster = new ExtendedClusterImpl(new ClusterConfig()
                                                                          .memberAlias("broker2")
                                                                          .transport(cfg -> cfg
                                                                                  .transportFactory(new TcpTransportFactory()))
                                                                          .membership(ship -> ship.seedMembers(seed))
            );
            cluster.startAwait();
            address2 = cluster.address();
            broker2 = new ScalecubeEventBusBroker(cluster);
            eventBus2 = new BrokerEventBus();
            eventBus2.addBroker(broker2);
        }

        {
            ExtendedClusterImpl cluster = new ExtendedClusterImpl(new ClusterConfig()
                                                                          .memberAlias("broker3")
                                                                          .transport(cfg -> cfg.transportFactory(new TcpTransportFactory()))
                                                                          .membership(ship -> ship.seedMembers(seed))
            );
            cluster.startAwait();
            address3 = cluster.address();
            broker3 = new ScalecubeEventBusBroker(cluster);
            eventBus3 = new BrokerEventBus();
            eventBus3.addBroker(broker3);
        }

        System.out.println(address1);
        System.out.println(address2);
        System.out.println(address3);

    }

    @Test
    @SneakyThrows
    public void test() {
        int total = 1_000;

        Duration d = Flux.merge(
                                 eventBus1
                                         .subscribe(Subscription
                                                            .builder()
                                                            .justBroker()
                                                            .topics("/test")
                                                            .subscriberId("test")
                                                            .build(), String.class)
                                         .map(s -> "from1@" + s),
                                 eventBus2
                                         .subscribe(Subscription
                                                            .builder()
                                                            .justBroker()
                                                            .topics("/test")
                                                            .subscriberId("test")
                                                            .build(), String.class)
                                         .map(s -> "from2@" + s)
                         )
                .doOnSubscribe(s -> Mono
                        .delay(Duration.ofSeconds(1))
                        .thenMany(
                                Flux.range(0, total/2)
                                    .flatMap(i -> eventBus3.publish("/test", "hello:" + i))
                        )
                        .subscribe())

                .take(total)
                .timeout(Duration.ofSeconds(10))
//                .doOnNext(s -> System.out.println(s))
                .count()
                .doOnNext(s -> {
                    System.out.println(s);
                })
                .as(StepVerifier::create)
                .expectNext(Long.valueOf(total))
                .verifyComplete();

        System.out.println(d);

    }
}