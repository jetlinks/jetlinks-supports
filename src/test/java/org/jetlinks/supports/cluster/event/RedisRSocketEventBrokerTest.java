package org.jetlinks.supports.cluster.event;

import lombok.SneakyThrows;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.ipc.IpcDefinition;
import org.jetlinks.core.ipc.IpcInvoker;
import org.jetlinks.core.ipc.IpcInvokerBuilder;
import org.jetlinks.core.ipc.IpcService;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.event.BrokerEventBus;
import org.jetlinks.supports.ipc.EventBusIpcService;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class RedisRSocketEventBrokerTest {

    ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate = RedisHelper.getRedisTemplate();
    Disposable.Composite disposable = Disposables.composite();

    static {
        //ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

//    @After
//    public void shutdown() {
//        disposable.dispose();
//        reactiveRedisTemplate.execute(connection -> {
//            return connection.serverCommands().flushDb();
//        }).blockLast();
//    }


    @Test
    @SneakyThrows
    public void testIpc() {
        BrokerEventBus eventBus = new BrokerEventBus();
        BrokerEventBus eventBus2 = new BrokerEventBus();

        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis3", "ipc-bus1", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus.addBroker(new RedisRSocketEventBroker(clusterManager,
                                                           reactiveRedisTemplate.getConnectionFactory(),
                                                           RSocketAddress.of(1234)));
            disposable.add(clusterManager::shutdown);
        }

        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis3", "ipc-bus2", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus2.addBroker(new RedisRSocketEventBroker(clusterManager,
                                                            reactiveRedisTemplate.getConnectionFactory(),
                                                            RSocketAddress.of(1235)));
            disposable.add(clusterManager::shutdown);
        }
        Thread.sleep(2000);
        IpcService ipcService = new EventBusIpcService(1, eventBus);
        IpcService ipcService2 = new EventBusIpcService(2, eventBus2);

        IpcDefinition<String,String> definition =  IpcDefinition.of("org.jetlinks.test", String.class, String.class);
        ipcService2.listen(definition, IpcInvokerBuilder.forRequest("test", a->Mono.just(a.toLowerCase())));

        IpcInvoker<String, String> invoker = ipcService.createInvoker("test", IpcDefinition.of("org.jetlinks.test", String.class, String.class));

        invoker.request("TEST")
               .as(StepVerifier::create)
               .expectNext("test")
               .verifyComplete();

    }

    @Test
    @SneakyThrows
    public void test() {
        BrokerEventBus eventBus = new BrokerEventBus();
        eventBus.setPublishScheduler(Schedulers.parallel());
        BrokerEventBus eventBus2 = new BrokerEventBus();
        eventBus2.setPublishScheduler(Schedulers.parallel());
        BrokerEventBus eventBus3 = new BrokerEventBus();
        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis2", "test-bus1", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus.addBroker(new RedisRSocketEventBroker(clusterManager,
                                                           reactiveRedisTemplate.getConnectionFactory(),
                                                           RSocketAddress.of(1234)));
            disposable.add(clusterManager::shutdown);
        }

        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis2", "test-bus2", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus2.addBroker(new RedisRSocketEventBroker(clusterManager,
                                                            reactiveRedisTemplate.getConnectionFactory(),
                                                            RSocketAddress.of(1235)));
            disposable.add(clusterManager::shutdown);
        }


        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis2", "test-bus3", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus3.addBroker(new RedisRSocketEventBroker(clusterManager,
                                                            reactiveRedisTemplate.getConnectionFactory(),
                                                            RSocketAddress.of(1236)));
            disposable.add(clusterManager::shutdown);
        }

        Subscription subscription = Subscription.of("test",
                                                    new String[]{"/test/topic1"}
                , Subscription.Feature.broker
                , Subscription.Feature.local
                                                    //, Subscription.Feature.shared
        );

        AtomicReference<Long> startWith = new AtomicReference<>();

        Flux.merge(
                eventBus.subscribe(subscription)
                , eventBus.subscribe(subscription)
                , eventBus2.subscribe(subscription)
                , eventBus3.subscribe(subscription)
        )
            .doOnSubscribe(sub -> {
                Mono.delay(Duration.ofSeconds(1))
                    .doOnNext(i -> startWith.set(System.currentTimeMillis()))
                    .thenMany(Flux.range(0, 10)
                                  .flatMap(l -> eventBus2.publish("/test/topic1", new ReadPropertyMessage())))
                    .subscribe();
            })
            .take(40L)
            .timeout(Duration.ofSeconds(10))
            .doOnNext(payload -> payload.bodyToString(true))
            .count()
            .as(StepVerifier::create)
            .expectNext(40L)
            .verifyComplete();
        System.out.println(System.currentTimeMillis() - startWith.get());
    }
}