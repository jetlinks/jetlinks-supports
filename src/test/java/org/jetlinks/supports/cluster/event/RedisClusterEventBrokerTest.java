package org.jetlinks.supports.cluster.event;


import lombok.SneakyThrows;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.event.BrokerEventBus;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class RedisClusterEventBrokerTest {


    ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate = RedisHelper.getRedisTemplate();


    @Test
    @SneakyThrows
    public void test() {

        BrokerEventBus eventBus = new BrokerEventBus();

        BrokerEventBus eventBus2 = new BrokerEventBus();

        BrokerEventBus eventBus3 = new BrokerEventBus();

        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis", "test-bus1", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus.addBroker(new RedisClusterEventBroker(clusterManager, reactiveRedisTemplate.getConnectionFactory()));
        }
        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis", "test-bus2", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus2.addBroker(new RedisClusterEventBroker(clusterManager, reactiveRedisTemplate.getConnectionFactory()));
        }

        {
            RedisClusterManager clusterManager = new RedisClusterManager("redis", "test-bus3", reactiveRedisTemplate);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus3.addBroker(new RedisClusterEventBroker(clusterManager, reactiveRedisTemplate.getConnectionFactory()));
        }

        Subscription subscription = Subscription.of("test",
                new String[]{"/test/topic1"}
                , Subscription.Feature.broker
                , Subscription.Feature.local
                , Subscription.Feature.shared
        );

         Flux.merge(
                        eventBus.subscribe(subscription)
//                        , eventBus2.subscribe(subscription)
                        , eventBus3.subscribe(subscription)
                )
                .doOnSubscribe(sub -> {
                    Mono.delay(Duration.ofSeconds(1))
                            .thenMany(Flux.range(0, 100)
                                    .flatMap(l -> {
                                        return eventBus2.publish("/test/topic1", "hello"+l);
                                    }))
                            .subscribe();
                })
                .take(Duration.ofSeconds(2))
                .map(payload -> payload.getPayload().bodyAsString())
                .as(StepVerifier::create)
                .expectNextCount(100L)
                .verifyComplete();

    }

}