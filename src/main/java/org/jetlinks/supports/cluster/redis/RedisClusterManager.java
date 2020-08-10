package org.jetlinks.supports.cluster.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.*;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("all")
@Slf4j
public class RedisClusterManager implements ClusterManager {

    private String clusterName;

    private String serverId;

    private Map<String, RedisClusterQueue> queues = new ConcurrentHashMap<>();
    private Map<String, ClusterTopic> topics = new ConcurrentHashMap<>();
    private Map<String, ClusterCache> caches = new ConcurrentHashMap<>();
    private Map<String, ClusterSet> sets = new ConcurrentHashMap<>();

    private ReactiveRedisTemplate<?, ?> commonOperations;

    private RedisHaManager haManager;

    private RedisClusterNotifier notifier;

    private ReactiveRedisOperations<String, String> stringOperations;

    private ReactiveRedisTemplate<String, ?> queueRedisTemplate;

    public RedisClusterManager(String name, ServerNode serverNode, ReactiveRedisTemplate<?, ?> operations) {
        this.clusterName = name;
        this.commonOperations = operations;
        this.notifier = new RedisClusterNotifier(name, serverNode.getId(), this);
        this.serverId = serverNode.getId();
        this.haManager = new RedisHaManager(name, serverNode, this, (ReactiveRedisTemplate) operations);
        this.stringOperations = new ReactiveRedisTemplate<>(operations.getConnectionFactory(), RedisSerializationContext.string());

        this.queueRedisTemplate = new ReactiveRedisTemplate<>(operations.getConnectionFactory(),
                RedisSerializationContext.<String, Object>newSerializationContext()
                        .key(RedisSerializer.string())
                        .value((RedisSerializationContext.SerializationPair<Object>) operations.getSerializationContext().getValueSerializationPair())
                        .hashKey(RedisSerializer.string())
                        .hashValue(operations.getSerializationContext().getHashValueSerializationPair())
                        .build());
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

        //定时尝试拉取队列数据
        Flux.interval(Duration.ofSeconds(5))
                .flatMap(i -> Flux.fromIterable(queues.values()))
                .subscribe(RedisClusterQueue::tryPoll);

        this.queueRedisTemplate
                .<String>listenToPattern("queue:data:produced")
                .doOnError(err->{
                    log.error(err.getMessage(),err);
                })
                .subscribe(sub -> {
                    RedisClusterQueue queue = queues.get(sub.getMessage());
                    if (queue != null) {
                        queue.tryPoll();
                    }
                });
    }

    public void shutdown() {
        this.haManager.shutdown();
    }

    @Override
    public HaManager getHaManager() {
        return haManager;
    }

    @SuppressWarnings("all")
    protected <K, V> ReactiveRedisTemplate<K, V> getRedis() {
        return (ReactiveRedisTemplate<K, V>) commonOperations;
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
        return queues.computeIfAbsent(queueId, id -> new RedisClusterQueue<>(id, this.queueRedisTemplate));
    }

    @Override
    public <T> ClusterTopic<T> getTopic(String topic) {
        return topics.computeIfAbsent(topic, id -> new RedisClusterTopic(id, this.getRedis()));
    }

    @Override
    public <K, V> ClusterCache<K, V> getCache(String cache) {
        return caches.computeIfAbsent(cache, id -> new RedisClusterCache<K, V>(id, this.getRedis()));
    }

    @Override
    public <V> ClusterSet<V> getSet(String name) {
        return sets.computeIfAbsent(name, id -> new RedisClusterSet<V>(id, this.getRedis()));
    }

    @Override
    public ClusterCounter getCounter(String name) {
        return new RedisClusterCounter(stringOperations, clusterName + ":counter:" + name);
    }
}
