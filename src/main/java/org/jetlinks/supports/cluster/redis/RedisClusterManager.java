package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.*;
import org.springframework.data.redis.core.ReactiveRedisOperations;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("all")
public class RedisClusterManager implements ClusterManager {

    private String clusterName;

    private String serverId;

    private Map<String, ClusterQueue> queues = new ConcurrentHashMap<>();
    private Map<String, ClusterTopic> topics = new ConcurrentHashMap<>();
    private Map<String, ClusterCache> caches = new ConcurrentHashMap<>();
    private Map<String, HaManager> haManagers = new ConcurrentHashMap<>();

    private ReactiveRedisOperations<?, ?> commonOperations;

    private RedisClusterNotifier notifier;

    public RedisClusterManager(String name, String serverId, ReactiveRedisOperations<?, ?> operations) {
        this.clusterName = name;
        this.commonOperations = operations;
        this.notifier = new RedisClusterNotifier(name, serverId, this);
        this.serverId=serverId;

    }

    @Override
    public String getCurrentServerId() {
        return serverId;
    }

    public void startup() {
        notifier.startup();
    }

    public void shutdown(){

    }

    @SuppressWarnings("all")
    protected <K, V> ReactiveRedisOperations<K, V> getRedis() {
        return (ReactiveRedisOperations<K, V>) commonOperations;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    public ClusterNotifier getNotifier() {
        return notifier;
    }

    @Override
    public <T> ClusterQueue<T> getQueue(String queueId) {
        return queues.computeIfAbsent(queueId, id -> new RedisClusterQueue<>(queueId, this.getRedis()));
    }

    @Override
    public <T> ClusterTopic<T> getTopic(String topic) {
        return topics.computeIfAbsent(topic, id -> new RedisClusterTopic(topic, this.getRedis()));
    }

    @Override
    public <K, V> ClusterCache<K, V> getCache(String cache) {
        return caches.computeIfAbsent(cache, id -> new RedisClusterCache<K,V>(cache, this.getRedis()));
    }
}
