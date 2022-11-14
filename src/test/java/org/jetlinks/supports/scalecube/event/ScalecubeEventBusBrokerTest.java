package org.jetlinks.supports.scalecube.event;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.supports.event.BrokerEventBus;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class ScalecubeEventBusBrokerTest {
    @Test
    public void testParallelStart() {

        int port1 = ThreadLocalRandom.current().nextInt(3000, 20000);
        int port2 = ThreadLocalRandom.current().nextInt(3000, 20000);
        BrokerEventBus eventBus1, eventBus2;

        Flux<TopicPayload> data;
        {
            ExtendedClusterImpl cluster = new ExtendedClusterImpl(new ClusterConfig()
                                                                          .memberAlias("p1")
                                                                          .membership(conf -> conf.seedMembers(Address.create("127.0.0.1", port2)))
                                                                          .transport(cfg -> cfg
                                                                                  .port(port1)
                                                                                  .transportFactory(new TcpTransportFactory()))
            );
            cluster.startAwait();
            eventBus1 = new BrokerEventBus();
            eventBus1.addBroker(new ScalecubeEventBusBroker(cluster));

        }

        {
            ExtendedClusterImpl cluster = new ExtendedClusterImpl(new ClusterConfig()
                                                                          .memberAlias("p2")
                                                                          .membership(conf -> conf.seedMembers(Address.create("127.0.0.1", port1)))
                                                                          .transport(cfg -> cfg
                                                                                  .port(port2)
                                                                                  .transportFactory(new TcpTransportFactory()))
            );
            cluster.startAwait();
            eventBus2 = new BrokerEventBus();
            eventBus2.addBroker(new ScalecubeEventBusBroker(cluster));
            data = eventBus2.subscribe(Subscription.of("test", "/test", Subscription.Feature.broker));
        }

        data.take(1)
            .take(Duration.ofSeconds(5))
            .doOnSubscribe(sub -> {
                Mono.delay(Duration.ofMillis(100))
                    .flatMap(ignore -> {
                        return eventBus1.publish("/test", 1);
                    })
                    .subscribe();
            })
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

    }
}