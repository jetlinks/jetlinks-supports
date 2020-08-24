package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.ClusterCache;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RedisClusterCache<K, V> implements ClusterCache<K, V> {

    private final ReactiveHashOperations<Object, K, V> hash;

    private ReactiveRedisOperations<Object, Object> redis;

    private final String redisKey;

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
    public Flux<Map.Entry<K, V>> get(Collection<K> key) {
        return hash.multiGet(redisKey, key)
                .flatMapIterable(list -> {
                    Object[] keyArr = key.toArray();
                    List<Map.Entry<K, V>> entries = new ArrayList<>(keyArr.length);
                    for (int i = 0; i < list.size(); i++) {
                        entries.add(new RedisSimpleEntry((K) keyArr[i], list.get(i)));
                    }
                    return entries;
                });
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
    public Mono<V> getAndRemove(K key) {
        // TODO: 2020/8/24 使用script实现?
        return hash.get(redisKey, key)
                .flatMap(v -> remove(key).thenReturn(v));
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
        if (CollectionUtils.isEmpty(multi)) {
            return Mono.just(true);
        }
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

    class RedisSimpleEntry implements Map.Entry<K, V> {
        K key;
        V value;

        RedisSimpleEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V old = getValue();
            if (value == null) {
                remove(getKey()).subscribe();
            } else {
                put(getKey(), this.value = value).subscribe();
            }
            return old;
        }
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
