package org.jetlinks.supports.cluster.redis;

import lombok.SneakyThrows;
import org.jetlinks.supports.cluster.RedisHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.tools.agent.ReactorDebugAgent;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisClusterQueueTest {

    private ReactiveRedisTemplate<String, Object> operations;

    static {
        //ReactorDebugAgent.init();
        //ReactorDebugAgent.processExistingClasses();
    }

    @Before
    public void init() {
        operations = new ReactiveRedisTemplate<>(RedisHelper.getRedisTemplate().getConnectionFactory(), RedisSerializationContext.<String, Object>newSerializationContext()
                .key(RedisSerializer.string())
                .value(RedisSerializer.java())
                .hashKey(RedisSerializer.string())
                .hashValue(RedisSerializer.java())
                .build());
    }

    @Test
    @SneakyThrows
    @Ignore
    public void testBenchmark() {
        RedisClusterQueue<Object> queue = new RedisClusterQueue<>("test2", operations);
        try {
//            queue.setPollMod(ClusterQueue.Mod.LIFO);
            queue.setLocalConsumerPercent(0F);
            int number = 10000;

            CountDownLatch latch = new CountDownLatch(number);
            Duration duration = Flux.range(0, number)
                    .<Object>map(i -> "aaaa" + i)
                    .as(queue::add)
                    .as(StepVerifier::create)
                    .expectNext(true)
                    .verifyComplete();
            System.out.println("add use time:" + duration);

            Assert.assertEquals(queue.size().block(), new Integer(number));

            long time = System.currentTimeMillis();
            queue.subscribe()
//                    .buffer(200)
//                    .log()
//                    .flatMap(Flux::fromIterable)
                    .doOnNext(i -> latch.countDown())
//                    .doOnNext(System.out::println)
                    .subscribe();

            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(queue.size().block(), new Integer(0));

            System.out.println(System.currentTimeMillis() - time + "ms");
        } finally {
            queue.operations.delete("test2").block();
        }
    }

    @Test
    public void test() {
        RedisClusterQueue<Integer> queue = new RedisClusterQueue("test3", operations);
        operations.delete("test3").block();
        queue.setLocalConsumerPercent(0F);
        queue.subscribe()
                .doOnSubscribe(sb->{
                   queue.add(Flux.range(0,100))
                           .subscribe();
                })
                .take(Duration.ofSeconds(3))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(100)
                .verifyComplete();
    }
    @Test
    public void testPoll() {
        RedisClusterQueue<Object> queue = new RedisClusterQueue("test", operations);

        queue.add(Mono.just("1234"))
                .subscribe();

        queue.poll()
                .as(StepVerifier::create)
                .expectNext("1234")
                .verifyComplete();
    }

}