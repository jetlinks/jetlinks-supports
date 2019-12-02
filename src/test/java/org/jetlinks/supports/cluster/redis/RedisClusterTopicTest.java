package org.jetlinks.supports.cluster.redis;

import lombok.SneakyThrows;
import org.jetlinks.supports.cluster.RedisHelper;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisClusterTopicTest {
    private ReactiveRedisTemplate<Object, Object> operations= RedisHelper.getRedisTemplate();

    @Test
    @SneakyThrows
    public void test() {
        RedisClusterTopic<Object> topic = new RedisClusterTopic("test", operations);
        int number = 10000;
        CountDownLatch countDownLatch = new CountDownLatch(number);

        topic.subscribe()
                .subscribe(r -> countDownLatch.countDown());

        Thread.sleep(100);
        topic.publish(Flux.range(0, number).map(Object.class::cast))
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

        Assert.assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));

    }

}