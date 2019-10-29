package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.ClusterCache;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

public class RedisClusterCache<K, V> implements ClusterCache<K, V> {

    private ReactiveHashOperations<Object, K, V> hash;

    private ReactiveRedisOperations<Object, Object> redis;

    private String redisKey;

    public RedisClusterCache(String redisKey, ReactiveRedisOperations<Object, Object> redis) {
        this(redisKey, redis.opsForHash());
        this.redis = redis;
    }

    private RedisClusterCache(String redisKey, ReactiveHashOperations<Object, K, V> hash) {
        this.hash = hash;
        this.redisKey = redisKey;
    }

    @Override
    public Mono<V> get(K key) {
        return hash.get(redisKey, key);
    }

    @Override
    public Flux<V> get(Collection<K> key) {
        return hash.multiGet(redisKey, key)
                .flatMapMany(Flux::fromIterable);
    }

    @Override
    public Mono<Boolean> put(K key, V value) {
        return hash.put(redisKey, key, value);
    }

    @Override
    public Mono<Boolean> putIfAbsent(K key, V value) {
        return hash.putIfAbsent(redisKey, key, value);
    }

    @Override
    public Mono<Boolean> remove(K key) {
        return hash.remove(redisKey, key)
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> remove(Collection<K> key) {
        return hash.remove(redisKey, key.toArray())
                .thenReturn(true);
    }

    @Override
    public Mono<Boolean> containsKey(K key) {
        return hash.hasKey(redisKey, key);
    }


    @Override
    public Flux<K> keys() {

        return hash.keys(redisKey);
    }

    @Override
    public Flux<V> values() {

        return hash.values(redisKey);
    }

    @Override
    public Mono<Boolean> putAll(Map<? extends K, ? extends V> multi) {
        return hash.putAll(redisKey, multi);
    }

    @Override
    public Mono<Integer> size() {
        return hash.size(redisKey)
                .map(Number::intValue);
    }

    @Override
    public Flux<Map.Entry<K, V>> entries() {
        return hash.scan(redisKey)
                .map(RedisHashEntry::new);
    }

    @Override
    public Mono<Void> clear() {
        return hash
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
