package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.ClusterSet;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveSetOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public class RedisClusterSet<V> implements ClusterSet<V> {

    private final ReactiveSetOperations<Object, Object> set;

    private final String redisKey;

    public RedisClusterSet(String redisKey, ReactiveRedisOperations<Object, Object> redis) {
        this(redisKey, redis.opsForSet());
    }

    private RedisClusterSet(String redisKey, ReactiveSetOperations<Object, Object> set) {
        this.set = set;
        this.redisKey = redisKey;
    }


    @Override
    public Mono<Boolean> add(V value) {
        return set.add(redisKey, value)
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> add(Collection<V> values) {
        return set.add(redisKey, values.toArray())
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> remove(V value) {
        return set
                .remove(redisKey, value)
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> remove(Collection<V> values) {
        return set
                .remove(redisKey, values.toArray())
                .thenReturn(true);
    }

    @Override
    public Flux<V> values() {
        return set.members(redisKey)
                .map(v -> (V) v);
    }
}
