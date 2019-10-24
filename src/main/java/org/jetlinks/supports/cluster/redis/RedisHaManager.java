package org.jetlinks.supports.cluster.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ClusterTopic;
import org.jetlinks.core.cluster.HaManager;
import org.jetlinks.core.cluster.ServerNode;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class RedisHaManager implements HaManager {
    private ServerNode current;

    private String haName;

    private ClusterTopic<ServerNode> offlineTopic;

    private ClusterManager clusterManager;

    private Map<String, ServerNode> allNode = new ConcurrentHashMap<>();

    private ReactiveRedisOperations<String, ServerNode> operations;

    private ClusterTopic<ServerNode> keepalive;

    private ReactiveHashOperations<String, String, ServerNode> inRedisNode;
    private String allNodeHashKey;


    private FluxProcessor<ServerNode, ServerNode> onlineProcessor = EmitterProcessor.create(false);
    private FluxProcessor<ServerNode, ServerNode> offlineProcessor = EmitterProcessor.create(false);
    private volatile boolean started = false;

    public RedisHaManager(String name,
                          ServerNode current,
                          ClusterManager clusterManager,
                          ReactiveRedisOperations<String, ServerNode> operations) {
        this.haName = name;
        this.current = current;
        this.clusterManager = clusterManager;
        this.operations = operations;
        this.inRedisNode = this.operations.opsForHash();
        this.offlineTopic = clusterManager.getTopic("__ha_offline_topic:".concat(haName));
        this.keepalive = clusterManager.getTopic("__ha_keepalive:".concat(haName));
        this.allNodeHashKey = "__ha_all_node:".concat(haName);

    }

    public void checkAlive() {
        current.setLastKeepAlive(System.currentTimeMillis());

        inRedisNode.put(allNodeHashKey, current.getId(), current)
        .subscribe();

        keepalive.publish(Mono.just(current)).subscribe();

        Map<String, ServerNode> maybeOffline = getAllNode()
                .stream()
                .filter(node -> System.currentTimeMillis() - node.getLastKeepAlive() > TimeUnit.SECONDS.toMillis(10))
                .filter(node -> !node.isSame(current))
                .collect(Collectors.toMap(ServerNode::getId, Function.identity()));

        //检查节点是否离线
        inRedisNode.keys(allNodeHashKey)
                .filter(maybeOffline::containsKey)
                .map(maybeOffline::get)
                .collectList()
                .filter(list -> !list.isEmpty())
                .flatMapMany(list -> inRedisNode
                        .remove(allNodeHashKey, list.stream().map(ServerNode::getId).toArray())
                        .thenMany(Flux.fromIterable(list))
                ).as(offlineTopic::publish)
                .subscribe();
    }

    public void shutdown() {
        offlineTopic
                .publish(Mono.just(current))
                .block();
    }

    public synchronized void startup() {
        if (started) {
            return;
        }
        started = true;
        allNode.put(current.getId(), current);

        //注册自己
        inRedisNode.put(allNodeHashKey, current.getId(), current)
                .flatMapMany(r -> inRedisNode.values(allNodeHashKey))
                .subscribe(node -> {
                    node.setLastKeepAlive(System.currentTimeMillis());
                    allNode.put(node.getId(), node);
                    Flux.interval(Duration.ZERO, Duration.ofSeconds(5))
                            .doOnNext(i -> this.checkAlive())
                            .subscribe();
                });

        offlineTopic.subscribe()
                .subscribe(serverNode -> {
                    //自己
                    if (currentServer().isSame(serverNode)) {
                        return;
                    }
                    if (allNode.remove(serverNode.getId()) != null) {
                        log.debug("[{}]:server node [{}] offline", haName, serverNode.getId());
                        //node offline
                        inRedisNode
                                .remove(allNodeHashKey, serverNode.getId())
                                .subscribe();
                        onlineProcessor.onNext(serverNode);
                    }
                });
        //其他节点定时发布
        keepalive.subscribe()
                .subscribe(serverNode -> {
                    serverNode.setLastKeepAlive(System.currentTimeMillis());
                    //自己
                    if (currentServer().getId().equals(serverNode.getId())) {
                        return;
                    }
                    if (!allNode.containsKey(serverNode.getId())) {
                        log.debug("[{}]:server node [{}] online", haName, serverNode.getId());
                        //node join
                        onlineProcessor.onNext(serverNode);
                    }
                    allNode.put(serverNode.getId(), serverNode);
                });


    }

    @Override
    public ServerNode currentServer() {
        return current;
    }

    @Override
    public Flux<ServerNode> subscribeServerOnline() {
        return onlineProcessor
                .filter(node -> !node.getId().equals(current.getId()));
    }

    @Override
    public Flux<ServerNode> subscribeServerOffline() {
        return offlineProcessor
                .filter(node -> !node.getId().equals(current.getId()));
    }

    @Override
    public List<ServerNode> getAllNode() {
        return new ArrayList<>(allNode.values());
    }
}
