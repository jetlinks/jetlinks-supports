package org.jetlinks.supports.cluster.redis;

import org.jetlinks.supports.cluster.RedisHelper;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

import static org.junit.Assert.*;

public class RedisClusterCacheTest {
    private static final Duration LIMIT = Duration.ofSeconds(10);

    @Test
    public void concurrentGetAndRemoveReturnsTheValueToOnlyOneReader() {
        RedisClusterCache<String, Object> cache = new RedisClusterCache<>(
            "get-remove-" + UUID.randomUUID(), RedisHelper.getRedisTemplate());
        try {
            assertTrue(cache.put("key", "value").block(LIMIT));
            StepVerifier.create(Flux.range(0, 64).flatMap(i -> cache.getAndRemove("key"), 64).collectList())
                .assertNext(values -> {
                    assertEquals(1, values.size());
                    assertEquals("value", values.get(0));
                }).expectComplete().verify(LIMIT);
            StepVerifier.create(cache.getAndRemove("key")).expectComplete().verify(LIMIT);
            StepVerifier.create(cache.get("key")).expectComplete().verify(LIMIT);
        } finally {
            cache.clear().block(LIMIT);
        }
    }

    @Test
    public void scriptUsesTheHashFieldAndHashValueSerializers() {
        ReactiveRedisTemplate<Object, Object> redis = new ReactiveRedisTemplate<>(
            RedisHelper.getRedisTemplate().getConnectionFactory(),
            RedisSerializationContext.<Object, Object>newSerializationContext(RedisSerializer.java())
                .hashKey(RedisSerializer.string()).hashValue(RedisSerializer.string()).build());
        RedisClusterCache<String, String> cache = new RedisClusterCache<>("hash-serializers-" + UUID.randomUUID(), redis);
        try {
            assertTrue(cache.put("field", "值").block(LIMIT));
            StepVerifier.create(cache.getAndRemove("field")).expectNext("值").expectComplete().verify(LIMIT);
            StepVerifier.create(cache.getAndRemove("field")).expectComplete().verify(LIMIT);
        } finally {
            cache.clear().block(LIMIT);
        }
    }
}
