package org.jetlinks.supports.cluster.redis;

import lombok.SneakyThrows;
import org.jetlinks.supports.cluster.RedisHelper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisClusterQueueTest {

    private ReactiveRedisTemplate<Object, Object> operations= RedisHelper.getRedisTemplate();


    @Test
    @SneakyThrows
    @Ignore
    public void testBenchmark() {
        RedisClusterQueue<Object> queue = new RedisClusterQueue("test2", operations);
        queue.setLocalConsumerPercent(0F);
        int number = 10000;

        CountDownLatch latch = new CountDownLatch(number);
        queue.subscribe()
                .subscribe(r -> latch.countDown());
        long time = System.currentTimeMillis();
        Flux.range(0, number)
                .<Object>map(i -> "aaaa" + i)
                .as(queue::add)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();
        System.out.println("add done");
        System.out.println(System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
        Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    public void test() {
        RedisClusterQueue<Object> queue = new RedisClusterQueue("test", operations);

        queue.add(Mono.just("1234"))
                .subscribe();

        queue.poll()
                .as(StepVerifier::create)
                .expectNext("1234")
                .verifyComplete();
    }

}