package org.jetlinks.supports.scalecube;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.services.Microservices;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ScalecubeTest {


    @Test
    @SneakyThrows
    public void test() {
// Start cluster node Alice as a seed node of the cluster, listen and print all incoming messages
        Cluster alice =
                new ClusterImpl()
                        .transportFactory(TcpTransportFactory::new)
                        .handler(
                                cluster -> {
                                    return new ClusterMessageHandler() {
                                        @Override
                                        public void onMessage(Message msg) {
                                            System.out.println("Alice received: " + msg.data());
                                            cluster
                                                    .send(msg.sender(), Message.fromData("Greetings from Alice"))
                                                    .subscribe(null, Throwable::printStackTrace);
                                        }
                                    };
                                })
                        .startAwait();

        // Join cluster node Bob to cluster with Alice, listen and respond for incoming greeting
        // messages
        Cluster bob =
                new ClusterImpl()
                        .membership(opts -> opts.seedMembers(alice.address()))
                        .transportFactory(TcpTransportFactory::new)
                        .handler(
                                cluster -> {
                                    return new ClusterMessageHandler() {
                                        @Override
                                        public void onMessage(Message msg) {
                                            System.out.println("Bob received: " + msg.data());
                                            cluster
                                                    .send(msg.sender(), Message.fromData("Greetings from Bob"))
                                                    .subscribe(null, Throwable::printStackTrace);
                                        }
                                    };
                                })
                        .startAwait();

        // Join cluster node Carol to cluster with Alice and Bob
        Cluster carol =
                new ClusterImpl()
                        .membership(opts -> opts.seedMembers(alice.address(), bob.address()))
                        .transportFactory(TcpTransportFactory::new)
                        .handler(
                                cluster -> {
                                    return new ClusterMessageHandler() {
                                        @Override
                                        public void onMessage(Message msg) {
                                            System.out.println("Carol received: " + msg.data());
                                        }

                                        @Override
                                        public void onMembershipEvent(MembershipEvent event) {
                                            System.out.println(event);
                                        }
                                    };
                                })
                        .startAwait();

        // Send from Carol greeting message to all other cluster members (which is Alice and Bob)
        Message greetingMsg = Message.fromData("Greetings from Carol");

        Flux.fromIterable(carol.otherMembers())
            .flatMap(member -> carol.send(member, greetingMsg))
            .subscribe(null, Throwable::printStackTrace);



        // Avoid exit main thread immediately ]:->
        Thread.sleep(1000);
    }
}
