package org.jetlinks.supports.cluster.redis;

import lombok.AllArgsConstructor;
import org.jetlinks.core.cluster.ClusterCounter;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

@AllArgsConstructor
public class RedisClusterCounter implements ClusterCounter {

    private final ReactiveRedisOperations<String, String> redis;

    private final String redisKey;

    @Override
    public Mono<Double> increment(double delta) {
        return redis
                .opsForValue()
                .increment(redisKey, delta);
    }

    @Override
    public Mono<Double> get() {
        return redis.opsForValue()
                    .get(redisKey)
                    .map(BigDecimal::new)
                    .map(Number::doubleValue)
                    .defaultIfEmpty(0D);
    }

    @Override
    public Mono<Double> set(double value) {
        return getAndSet(value);
    }

    @Override
    public Mono<Double> setAndGet(double value) {
        return redis
                .opsForValue()
                .set(redisKey, String.valueOf(value))
                .thenReturn(value);
    }

    @Override
    public Mono<Double> getAndSet(double value) {
        return redis
                .opsForValue()
                .getAndSet(redisKey, String.valueOf(value))
                .map(BigDecimal::new)
                .map(Number::doubleValue)
                .defaultIfEmpty(0D);
    }

    @Override
    public Mono<Double> remove() {
        return get()
                .flatMap(val -> redis
                        .delete(redisKey)
                        .thenReturn(val))
                ;
    }
}
