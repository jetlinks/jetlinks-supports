package org.jetlinks.supports.cluster.redis;

import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = TestApplication.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class RedisClusterTopicTest {
    @Autowired
    private ReactiveRedisTemplate<Object, Object> operations;

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