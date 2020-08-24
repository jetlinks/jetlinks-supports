package org.jetlinks.supports.cluster.redis;

import lombok.SneakyThrows;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ServerNode;
import org.jetlinks.supports.cluster.ClusterLocalCache;
import org.jetlinks.supports.cluster.RedisHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisClusterManagerTest {

    private ReactiveRedisTemplate<Object, Object> operations;

    public RedisClusterManager clusterManager;

    @Before
    public void init() {
        operations = RedisHelper.getRedisTemplate();

        clusterManager = new RedisClusterManager("default", "test", operations);
    }

    @Test
    @SneakyThrows
    public void testNotify() {
        RedisClusterNotifier notifier1 = new RedisClusterNotifier("test", "server-1", clusterManager);
        RedisClusterNotifier notifier2 = new RedisClusterNotifier("test", "server-2", clusterManager);
        notifier1.startup();
        notifier2.startup();
        int number = 100;
        CountDownLatch latch = new CountDownLatch(number);

        Disposable disposable1 = notifier1.handleNotify("test-no-reply")
                .subscribe(res -> latch.countDown());

        Thread.sleep(1000);
        Disposable disposable2 = notifier2.sendNotify("server-1", "test-no-reply", Flux.range(0, number))
                .subscribe();

        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        disposable2.dispose();
        disposable1.dispose();


        notifier1.handleNotify("test-reply", res -> Mono.just("pong"))
                .subscribe();
        notifier2.sendNotifyAndReceive("server-1", "test-reply", Mono.just("ping"))
                .timeout(Duration.ofSeconds(10))
                .as(StepVerifier::create)
                .expectNext("pong")
                .verifyComplete();

        notifier1.handleNotify("test_reply_multi", res -> Flux.just(1, 2, 3, 4).delayElements(Duration.ofMillis(100)))
                .subscribe();
        Thread.sleep(1000);
        notifier2.sendNotifyAndReceive("server-1", "test_reply_multi", Mono.just(1))
                .timeout(Duration.ofSeconds(10))
                .as(StepVerifier::create)
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    @Test
    public void testLocalCache() {
        ClusterCache<String, Object> cache = new ClusterLocalCache<>("test", clusterManager);
        cache.clear().block();

        cache.putAll(Collections.singletonMap("test", "123"))
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        cache.putAll(Collections.singletonMap("test2", "456"))
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();


        cache.get(Arrays.asList("test", "aaa", "test2"))
                .map(e -> e.getKey() + "=" + e.getValue())
                .as(StepVerifier::create)
                .expectNext("test=123", "aaa=null", "test2=456")
                .verifyComplete();

        cache.remove("test2")
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        cache.containsKey("test")
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        cache.get("test")
                .as(StepVerifier::create)
                .expectNext("123")
                .verifyComplete();

        cache.keys()
                .as(StepVerifier::create)
                .expectNext("test")
                .verifyComplete();

        cache.values()
                .as(StepVerifier::create)
                .expectNext("123")
                .verifyComplete();

        cache.entries()
                .map(Map.Entry::getValue)
                .as(StepVerifier::create)
                .expectNext("123")
                .verifyComplete();

        cache.remove("test")
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void testCache() {
        ClusterCache<String, Object> cache = clusterManager.getCache("test");
        cache.clear().block();
        cache.put("test", "123")
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        cache.containsKey("test")
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        cache.get("test")
                .as(StepVerifier::create)
                .expectNext("123")
                .verifyComplete();

        cache.keys()
                .as(StepVerifier::create)
                .expectNext("test")
                .verifyComplete();

        cache.values()
                .as(StepVerifier::create)
                .expectNext("123")
                .verifyComplete();

        cache.entries()
                .map(Map.Entry::getValue)
                .as(StepVerifier::create)
                .expectNext("123")
                .verifyComplete();

        cache.getAndRemove("test")
                .as(StepVerifier::create)
                .expectNext("123")
                .verifyComplete();


    }

    @Test
    @SneakyThrows
    public void testHaManager() {
        RedisHaManager haManager = new RedisHaManager("test",
                ServerNode.builder().id("test-node").build(),
                clusterManager,
                clusterManager.getRedis());
        haManager.startup();
        Assert.assertEquals(haManager.currentServer().getId(), "test-node");

        CountDownLatch latch = new CountDownLatch(2);
        haManager.subscribeServerOnline()
                .subscribe(node -> {
                    latch.countDown();
                    System.out.println(node.getId());
                });
        haManager.subscribeServerOffline()
                .subscribe(node -> {
                    latch.countDown();
                    System.out.println(node.getId());
                });

        Thread.sleep(1000);

        RedisHaManager haManager2 = new RedisHaManager("test",
                ServerNode.builder().id("test-node2").build(),
                clusterManager,
                clusterManager.getRedis());
        haManager2.startup();


        Thread.sleep(1000);
        haManager.shutdown();
        haManager2.shutdown();
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}