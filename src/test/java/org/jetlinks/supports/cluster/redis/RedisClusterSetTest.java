package org.jetlinks.supports.cluster.redis;

import org.jetlinks.supports.cluster.RedisHelper;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.test.StepVerifier;

public class RedisClusterSetTest {

    private ReactiveRedisTemplate<Object, Object> operations = RedisHelper.getRedisTemplate();


    @Test
    public void test() {
        RedisClusterSet<String> set = new RedisClusterSet<>("test-set", operations);

        set.add("1")
                .then(set.add("2"))
                .then(set.add("3"))
                .thenMany(set.values())
                .as(StepVerifier::create)
                .expectNextCount(3)
                .verifyComplete();

    }
}