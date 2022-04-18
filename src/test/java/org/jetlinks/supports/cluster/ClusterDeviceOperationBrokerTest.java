package org.jetlinks.supports.cluster;


import io.scalecube.cluster.ClusterConfig;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.ProductInfo;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.supports.device.session.LocalDeviceSessionManager;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.jetlinks.supports.test.InMemoryDeviceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterDeviceOperationBrokerTest {
    ClusterDeviceOperationBroker broker1;
    ClusterDeviceOperationBroker broker2;
    private DeviceRegistry registry;
    private DeviceOperator device;

    @Before
    @SneakyThrows
    public void init() {
        ExtendedClusterImpl cluster1 = new ExtendedClusterImpl(
                ClusterConfig.defaultConfig()
                             .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
        );
        cluster1.startAwait();

        ExtendedClusterImpl cluster2 = new ExtendedClusterImpl(
                ClusterConfig.defaultConfig()
                             .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                             .membership(ship -> ship.seedMembers(cluster1.address()))
        );
        cluster2.startAwait();


        Thread.sleep(1000);

        registry = InMemoryDeviceRegistry.create();

        registry.register(ProductInfo.builder()
                                     .id("test")
                                     .protocol("test")
                                     .metadata("{}")
                                     .build())
                .block();

        broker1 = new ClusterDeviceOperationBroker(cluster1, LocalDeviceSessionManager.create());
        broker2 = new ClusterDeviceOperationBroker(cluster2, LocalDeviceSessionManager.create());


        device = registry.register(DeviceInfo.builder()
                                             .id("test")
                                             .productId("test")
                                             .build())
                         .block();
    }


    @Test
    @SneakyThrows
    public void testSend() {
        AtomicInteger count = new AtomicInteger();

        broker2.handleSendToDeviceMessage(broker2.cluster.member().id())
               .doOnNext(msg -> {
                   count.incrementAndGet();
               })
               .subscribe();

        ReadPropertyMessage msg = new ReadPropertyMessage();
        msg.setDeviceId("test");
        msg.setMessageId("1");

        broker1.send(broker2.cluster.member().id(), Flux.just(msg))
               .as(StepVerifier::create)
               .expectNext(1)
               .verifyComplete();

        Thread.sleep(500);
        Assert.assertEquals(1, count.get());
    }

    @Test
    @SneakyThrows
    public void testReply() {
        AtomicInteger count = new AtomicInteger();

        broker2.handleReply("test", "2", Duration.ofSeconds(10))
               .doOnNext(msg -> {
                   count.incrementAndGet();
               })
               .subscribe();

        ReadPropertyMessage msg = new ReadPropertyMessage();
        msg.setDeviceId("test");
        msg.setMessageId("2");

        broker1.reply(msg.newReply().success())
               .as(StepVerifier::create)
               .expectNext(true)
               .verifyComplete();

        Thread.sleep(500);
        Assert.assertEquals(1, count.get());
    }


}