package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.*;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("all")
public class RedisClusterManager implements ClusterManager {

    private String clusterName;

    private String serverId;

    private Map<String, ClusterQueue> queues = new ConcurrentHashMap<>();
    private Map<String, ClusterTopic> topics = new ConcurrentHashMap<>();
    private Map<String, ClusterCache> caches = new ConcurrentHashMap<>();
    private Map<String, ClusterSet> sets = new ConcurrentHashMap<>();

    private ReactiveRedisOperations<?, ?> commonOperations;

    private RedisHaManager haManager;

    private RedisClusterNotifier notifier;

    private ReactiveRedisOperations<String, String> stringOperations;

    public RedisClusterManager(String name, ServerNode serverNode, ReactiveRedisTemplate<?, ?> operations) {
        this.clusterName = name;
        this.commonOperations = operations;
        this.notifier = new RedisClusterNotifier(name, serverNode.getId(), this);
        this.serverId = serverNode.getId();
        this.haManager = new RedisHaManager(name, serverNode, this, (ReactiveRedisTemplate) operations);
        this.stringOperations = new ReactiveRedisTemplate<>(operations.getConnectionFactory(), RedisSerializationContext.string());
    }

    public RedisClusterManager(String name, String serverId, ReactiveRedisTemplate<?, ?> operations) {
        this(name, ServerNode.builder().id(serverId).build(), operations);
    }

    @Override
    public String getCurrentServerId() {
        return serverId;
    }

    public void startup() {
        this.notifier.startup();
        this.haManager.startup();
    }

    public void shutdown() {
        this.haManager.shutdown();
    }

    @Override
    public HaManager getHaManager() {
        return haManager;
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
        return caches.computeIfAbsent(cache, id -> new RedisClusterCache<K, V>(cache, this.getRedis()));
    }

    @Override
    public <V> ClusterSet<V> getSet(String name) {
        return sets.computeIfAbsent(name, id -> new RedisClusterSet<V>(name, this.getRedis()));
    }

    @Override
    public ClusterCounter getCounter(String name) {
        return new RedisClusterCounter(stringOperations, clusterName + ":counter:" + name);
    }
}
