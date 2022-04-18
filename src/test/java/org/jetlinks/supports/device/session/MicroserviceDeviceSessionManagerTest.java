package org.jetlinks.supports.device.session;


import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.Microservices;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.ProductInfo;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.server.session.LostDeviceSession;
import org.jetlinks.supports.scalecube.DynamicServiceRegistry;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.jetlinks.supports.scalecube.ExtendedServiceDiscoveryImpl;
import org.jetlinks.supports.test.InMemoryDeviceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class MicroserviceDeviceSessionManagerTest {

    MicroserviceDeviceSessionManager manager1;
    MicroserviceDeviceSessionManager manager2;
    DeviceRegistry registry;

    DeviceOperator device;

    @Before
    @SneakyThrows
    public void init() {
        ExtendedClusterImpl cluster1 = new ExtendedClusterImpl(
                ClusterConfig.defaultConfig()
                             .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
        );
        cluster1.startAwait();

        Microservices microservices1 = Microservices
                .builder()
                .transport(RSocketServiceTransport::new)
                .services(MicroserviceDeviceSessionManager.createService(cluster1.member().id(),()->manager1))
                .serviceRegistry(new DynamicServiceRegistry())
                .discovery(serviceEndpoint -> new ExtendedServiceDiscoveryImpl(cluster1, serviceEndpoint))
                .startAwait();

        ExtendedClusterImpl cluster2 = new ExtendedClusterImpl(
                ClusterConfig.defaultConfig()
                             .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                             .membership(ship -> ship.seedMembers(cluster1.address()))
        );
        cluster2.startAwait();

        Microservices microservices2 = Microservices
                .builder()
                .transport(RSocketServiceTransport::new)
                .services(MicroserviceDeviceSessionManager.createService(cluster2.member().id(),()->manager2))
                .serviceRegistry(new DynamicServiceRegistry())
                .discovery(serviceEndpoint -> new ExtendedServiceDiscoveryImpl(cluster2, serviceEndpoint))
                .startAwait();

        manager1 = new MicroserviceDeviceSessionManager(cluster1,microservices1.call());
        manager2 = new MicroserviceDeviceSessionManager(cluster2,microservices2.call());

        manager1.init();
        manager2.init();
        Thread.sleep(1000);

        registry = InMemoryDeviceRegistry.create();

        registry.register(ProductInfo.builder()
                                     .id("test")
                                     .protocol("test")
                                     .metadata("{}")
                                     .build())
                .block();

        device = registry.register(DeviceInfo.builder()
                                             .id("test")
                                             .productId("test")
                                             .build())
                         .block();
    }

    @Test
    public void testRegisterInMulti() {
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }
        };
        AtomicInteger eventCount = new AtomicInteger();
        Disposables.composite(
                manager1.listenEvent(event -> {
                    if(event.isClusterExists()){
                        return Mono.empty();
                    }
                    eventCount.incrementAndGet();
                    return Mono.empty();
                }),
                manager2.listenEvent(event -> {
                    if(event.isClusterExists()){
                        return Mono.empty();
                    }
                    eventCount.incrementAndGet();
                    return Mono.empty();
                })
        );

        manager1.compute(session.getDeviceId(), mono -> Mono.just(session))
                .block();
        manager2.compute(session.getDeviceId(), mono -> Mono.just(session))
                .block();

        Assert.assertEquals(1, eventCount.get());
        eventCount.set(0);
        manager1.remove(device.getDeviceId(),false)
                .as(StepVerifier::create)
                .expectNext(2L)
                .verifyComplete();

        Assert.assertEquals(1, eventCount.get());
        Flux.merge(manager1.getSession(device.getDeviceId()),
                   manager2.getSession(device.getDeviceId()))
            .as(StepVerifier::create)
            .expectComplete()
            .verify();


    }

    @Test
    public void testRegister() {
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }
        };
        manager1.compute(session.getDeviceId(), mono -> Mono.just(session))
                .block();

        manager1.isAlive(session.getDeviceId())
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        Duration time = Flux.range(0,20000)
                .flatMap(i->manager2
                        .isAlive(session.getDeviceId()))
                .as(StepVerifier::create)
                .expectNextCount(20000)
                .verifyComplete();
        System.out.println(time);
    }
}