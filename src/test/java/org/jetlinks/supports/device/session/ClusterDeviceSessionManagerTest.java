package org.jetlinks.supports.device.session;


import io.scalecube.cluster.ClusterConfig;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.ProductInfo;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.LostDeviceSession;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.jetlinks.supports.scalecube.rpc.ScalecubeRpcManager;
import org.jetlinks.supports.test.InMemoryDeviceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.ContextView;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterDeviceSessionManagerTest {
    ClusterDeviceSessionManager manager1;
    ClusterDeviceSessionManager manager2;
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


        ExtendedClusterImpl cluster2 = new ExtendedClusterImpl(
            ClusterConfig.defaultConfig()
                         .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
                         .membership(ship -> ship.seedMembers(cluster1.address()))
        );
        cluster2.startAwait();

        ScalecubeRpcManager rpcManager = new ScalecubeRpcManager(cluster1, RSocketServiceTransport::new);
        ScalecubeRpcManager rpcManager2 = new ScalecubeRpcManager(cluster2, RSocketServiceTransport::new);
        rpcManager.startAwait();
        rpcManager2.startAwait();

        manager1 = new ClusterDeviceSessionManager(rpcManager);
        manager2 = new ClusterDeviceSessionManager(rpcManager2);

        manager1.init();
        manager2.init();
        Thread.sleep(2000);

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
    @SneakyThrows
    public void testConcurrency() {
        LostDeviceSession session = new LostDeviceSession("testConcurrency", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }

            @Override
            public Mono<Boolean> isAliveAsync() {
                return Reactors.ALWAYS_TRUE;
            }
        };

        AtomicInteger count = new AtomicInteger();
        manager1.listenEvent(e -> {
            if (e.getSession() == session) {
                count.incrementAndGet();
            }
            return Mono.empty();
        });
        AtomicInteger emptyCount = new AtomicInteger();
        AtomicInteger computeCount = new AtomicInteger();
        AtomicInteger resultCount = new AtomicInteger();

        int total = 2000;
        Flux.range(0, total)
            .flatMap(ignore -> Mono
                .defer(() -> Mono.zip(
                    manager1
                        .compute(session.getDeviceId(),
                                 Mono.fromSupplier(() -> {
                                     emptyCount.incrementAndGet();
                                     return session;
                                 }),
                                 old -> Mono.just(old)
                                            .delayElement(Duration.ofMillis(1))
                                            .doOnSuccess(s -> computeCount.incrementAndGet())
                        )
                        .subscribeOn(Schedulers.parallel())
                        .doOnNext(s -> resultCount.incrementAndGet())
                    ,
                    manager1.getSession(session.getDeviceId())
                            .subscribeOn(Schedulers.parallel())
                    //  ,
//                    manager1.removeLocalSession(session.getDeviceId())
//                             .subscribeOn(Schedulers.parallel())
                ))
                .subscribeOn(Schedulers.boundedElastic()))
            .then(Mono.delay(Duration.ofSeconds(1)))
            .then(Mono.fromSupplier(count::get))
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        Assert.assertEquals(1, emptyCount.get());
        Assert.assertEquals(total - 1, computeCount.get());
        Assert.assertEquals(total, resultCount.get());

        Flux.range(0, 10)
            .flatMap(i -> Mono.defer(() -> manager1.remove(session.getDeviceId(), true))
                              .subscribeOn(Schedulers.boundedElastic()))
            .then(Mono.fromSupplier(count::get))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void testRegisterInMulti() {
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }

            @Override
            public Mono<Boolean> isAliveAsync() {
                return Reactors.ALWAYS_TRUE;
            }
        };
        AtomicInteger eventCount = new AtomicInteger();
        Disposable disposable = Disposables.composite(
            manager1.listenEvent(event -> {
                System.err.println(event.getType() + " manager 1: " + event.isClusterExists());
                if (event.isClusterExists()) {
                    return Mono.empty();
                }
                eventCount.incrementAndGet();
                return Mono.empty();
            }),
            manager2.listenEvent(event -> {
                System.err.println(event.getType() + " manager 2: " + event.isClusterExists());
                if (event.isClusterExists()) {
                    return Mono.empty();
                }
                eventCount.incrementAndGet();
                return Mono.empty();
            })
        );

        Flux.concat(
                manager1.compute(session.getDeviceId(), mono -> Mono.just(session)),
                manager1.compute(session.getDeviceId(), mono -> Mono.just(session))
            )
            .blockLast();
        Thread.sleep(1000);
        manager2.compute(session.getDeviceId(), Mono.just(session), null)
                .block();
        Thread.sleep(1000);
        Assert.assertEquals(1, eventCount.get());
        eventCount.set(0);
        manager1.remove(device.getDeviceId(), false)
                .as(StepVerifier::create)
                .expectNext(2L)
                .verifyComplete();

        Assert.assertEquals(1, eventCount.get());
        Flux.merge(manager1.getSession(device.getDeviceId()),
                   manager2.getSession(device.getDeviceId()))
            .as(StepVerifier::create)
            .expectComplete()
            .verify();
        disposable.dispose();

    }

    @Test
    public void testRegisterRecursive() {
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }

            @Override
            public Mono<Boolean> isAliveAsync() {
                return Reactors.ALWAYS_TRUE;
            }
        };

        manager1.compute(session.getDeviceId(), mono -> {
                    return Mono
                        .delay(Duration.ofSeconds(1))
                        .then(Mono.defer(() -> manager1
                            .getSession(session.getDeviceId())))
                        .thenReturn(session);
                })
                .block();

        manager1
            .getSession(session.getDeviceId())
            .as(StepVerifier::create)
            .expectNext(session)
            .verifyComplete();

    }

    @Test
    public void testRegister() {
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }

            @Override
            public Mono<Boolean> isAliveAsync() {
                return Reactors.ALWAYS_TRUE;
            }
        };
        manager1.compute(session.getDeviceId(), mono -> Mono.just(session))
                .block();

        manager1.isAlive(session.getDeviceId())
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        Duration time = Flux.range(0, 20000)
                            .flatMap(i -> manager2
                                .isAlive(session.getDeviceId()))
                            .as(StepVerifier::create)
                            .expectNextCount(20000)
                            .verifyComplete();
        System.out.println(time);

        manager2.getSessionInfo()
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    public void testRemoveWhenRegister() {
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }

            @Override
            public Mono<Boolean> isAliveAsync() {
                return Reactors.ALWAYS_TRUE;
            }
        };
        manager1.compute(
                    session.getDeviceId(),
                    old -> old
                        .then(manager1.remove(session.getDeviceId(), true))
                        .thenReturn(session))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        manager1.getSession(session.getDeviceId())
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        manager1.compute(
                    session.getDeviceId(),
                    old -> old
                        .then(manager1.remove(session.getDeviceId(), true))
                        .then(Mono.empty()))
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();

        manager1
            .getSession(session.getDeviceId())
            .as(StepVerifier::create)
            .expectNextCount(0)
            .verifyComplete();
    }

    @Test
    public void testRemoveWhenGet() {
        AtomicBoolean alive = new AtomicBoolean(true);
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return alive.get();
            }

        };
        manager1.compute(
                    session.getDeviceId(),
                    old -> Mono.just(session))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();
        alive.set(false);

        manager1.compute(
                    session.getDeviceId(),
                    old -> manager1
                        .getSession(session.getDeviceId())
                        .then(Mono.fromRunnable(() -> alive.set(true)))
                        .<DeviceSession>thenReturn(session)
                        .subscribeOn(Schedulers.parallel()))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    public void testContext() {
        AtomicReference<ContextView> alive = new AtomicReference<>();
        LostDeviceSession session = new LostDeviceSession("test", device, DefaultTransport.MQTT) {
            @Override
            public boolean isAlive() {
                return true;
            }

        };
        manager1.listenEvent(e->{
            return Mono.deferContextual(ctx->{
                alive.set(ctx);
                return Mono.empty();
            });
        });

        manager1.compute(
                    session.getDeviceId(),
                    old -> Mono.just(session))
            .contextWrite(ctx->ctx.put("test",123))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        Assert.assertNotNull(alive.get());
        Assert.assertEquals(123, Objects.requireNonNull(alive.get().getOrDefault("test", 0)).intValue());
    }
}