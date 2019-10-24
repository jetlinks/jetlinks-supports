package org.jetlinks.supports.cluster.redis;

import lombok.SneakyThrows;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ServerNode;
import org.jetlinks.supports.cluster.ClusterLocalCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = TestApplication.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class RedisClusterManagerTest {

    @Autowired
    private ReactiveRedisTemplate<Object, Object> operations;

    public RedisClusterManager clusterManager;

    @Before
    public void init() {
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

        notifier1.handleNotify("test-reply", res -> Mono.just("pong"));

        notifier2.sendNotifyAndReceive("server-1", "test-reply", Mono.just("ping"))
                .timeout(Duration.ofSeconds(10))
                .as(StepVerifier::create)
                .expectNext("pong")
                .verifyComplete();
    }

    @Test
    public void testLocalCache(){
        ClusterCache<String, Object> cache = new ClusterLocalCache<>("test",clusterManager);

        cache.putAll(Collections.singletonMap("test", "123"))
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

        cache.remove("test")
                .as(StepVerifier::create)
                .expectNext(true)
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