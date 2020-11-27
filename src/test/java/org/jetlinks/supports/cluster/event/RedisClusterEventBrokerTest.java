package org.jetlinks.supports.cluster.event;


import lombok.SneakyThrows;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.event.BrokerEventBus;
import org.junit.After;
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

public class RedisClusterEventBrokerTest {


    ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate = RedisHelper.getRedisTemplate();
    Disposable.Composite disposable = Disposables.composite();

    @After
    public void shutdown() {
        disposable.dispose();
    }

    @Test
    @SneakyThrows
    public void test() {

        BrokerEventBus eventBus = new BrokerEventBus();

        BrokerEventBus eventBus2 = new BrokerEventBus();
        eventBus2.setPublishScheduler(Schedulers.parallel());
        BrokerEventBus eventBus3 = new BrokerEventBus();


        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis", "test-bus1", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus.addBroker(new RedisClusterEventBroker(clusterManager, reactiveRedisTemplate.getConnectionFactory()));
            disposable.add(clusterManager::shutdown);
        }
        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis", "test-bus2", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus2.addBroker(new RedisClusterEventBroker(clusterManager, reactiveRedisTemplate.getConnectionFactory()));
            disposable.add(clusterManager::shutdown);
        }

        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis", "test-bus3", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus3.addBroker(new RedisClusterEventBroker(clusterManager, reactiveRedisTemplate.getConnectionFactory()));
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
            .take(Duration.ofSeconds(10))
            .map(payload -> payload.bodyToString(true))
            .as(StepVerifier::create)
            .expectNextCount(30L)
            .verifyComplete();
        System.out.println(System.currentTimeMillis() - startWith.get());

    }

}