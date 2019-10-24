package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.ClusterCache;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

public class RedisClusterCache<K, V> implements ClusterCache<K, V> {

    private ReactiveHashOperations<Object, K, V> operations;

    private String redisKey;

    public RedisClusterCache(String redisKey, ReactiveRedisOperations<Object, Object> operations) {
        this(redisKey, operations.opsForHash());
    }

    public RedisClusterCache(String redisKey, ReactiveHashOperations<Object, K, V> operations) {
        this.operations = operations;
        this.redisKey = redisKey;
    }

    @Override
    public Mono<V> get(K key) {
        return operations.get(redisKey, key);
    }

    @Override
    public Flux<V> get(Collection<K> key) {
        return operations.multiGet(redisKey, key)
                .flatMapMany(Flux::fromIterable);
    }

    @Override
    public Mono<Boolean> put(K key, V value) {
        return operations.put(redisKey, key, value);
    }

    @Override
    public Mono<Boolean> remove(K key) {
        return operations.remove(redisKey, key)
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> remove(Collection<K> key) {
        return operations.remove(redisKey,key.toArray())
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> containsKey(K key) {
        return operations.hasKey(redisKey, key);
    }


    @Override
    public Flux<K> keys() {

        return operations.keys(redisKey);
    }

    @Override
    public Flux<V> values() {

        return operations.values(redisKey);
    }

    @Override
    public Mono<Boolean> putAll(Map<? extends K, ? extends V> multi) {
        return operations.putAll(redisKey, multi);
    }

    @Override
    public Mono<Integer> size() {
        return operations.size(redisKey)
                .map(Number::intValue);
    }

    @Override
    public Flux<Map.Entry<K, V>> entries() {
        return operations.scan(redisKey)
                .map(RedisHashEntry::new);
    }

    @Override
    public Mono<Void> clear() {
        return operations
                .delete(redisKey)
                .then();
    }

    class RedisHashEntry implements Map.Entry<K, V> {
        Map.Entry<K, V> entry;
        V value;

        RedisHashEntry(Map.Entry<K, V> entry) {
            this.entry = entry;
            this.value = entry.getValue();
        }

        @Override
        public K getKey() {
            return entry.getKey();
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V old = getValue();
            put(getKey(), this.value = value).subscribe();
            return old;
        }
    }
}
