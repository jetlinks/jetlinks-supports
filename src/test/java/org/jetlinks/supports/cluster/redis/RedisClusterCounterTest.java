package org.jetlinks.supports.cluster.redis;

import org.jetlinks.supports.cluster.RedisHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import reactor.test.StepVerifier;

import static org.junit.Assert.*;

public class RedisClusterCounterTest {

    private ReactiveRedisTemplate<String, String> operations;

    @Before
    public void init() {
        operations = new ReactiveRedisTemplate<>(RedisHelper.getRedisTemplate().getConnectionFactory(),
                RedisSerializationContext.string()
                );
    }

    @After
    public void shutdown(){
        operations
                .delete("test:counter")
                .then()
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    public void test() {
        RedisClusterCounter clusterCounter = new RedisClusterCounter(
                operations, "test:counter"
        );

        for (int i = 0; i < 100; i++) {
            clusterCounter.increment()
                    .as(StepVerifier::create)
                    .expectNext(1D)
                    .verifyComplete();

            clusterCounter.decrement()
                    .as(StepVerifier::create)
                    .expectNext(0D)
                    .verifyComplete();

            clusterCounter.increment(10.88)
                    .as(StepVerifier::create)
                    .expectNext(10.88)
                    .verifyComplete();

            clusterCounter.decrement(10.88)
                    .as(StepVerifier::create)
                    .expectNext(0D)
                    .verifyComplete();

            clusterCounter.getAndSet(99.99)
                    .as(StepVerifier::create)
                    .expectNext(0D)
                    .verifyComplete();

            clusterCounter.setAndGet(0D)
                    .as(StepVerifier::create)
                    .expectNext(0D)
                    .verifyComplete();
        }


    }
}