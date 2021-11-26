package org.jetlinks.supports.cluster.event;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.Payload;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ServerNode;
import org.jetlinks.core.codec.Codec;
import org.jetlinks.core.codec.Codecs;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.supports.event.EventBroker;
import org.jetlinks.supports.event.EventConnection;
import org.jetlinks.supports.event.EventConsumer;
import org.jetlinks.supports.event.EventProducer;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractClusterEventBroker implements EventBroker {

    protected final ReactiveRedisOperations<String, byte[]> redis;

    private final String id;

    private final Sinks.Many<EventConnection> eventConnectionMany = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

    private final Map<String, ClusterConnecting> connections = new ConcurrentHashMap<>();

    protected final ClusterManager clusterManager;

    private boolean started = false;

    protected final Codec<Subscription> subscriptionCodec = Codecs.lookup(Subscription.class);

    protected final Disposable.Composite disposable = Disposables.composite();

    public AbstractClusterEventBroker(ClusterManager clusterManager,
                                      ReactiveRedisConnectionFactory factory) {
        this.id = clusterManager.getClusterName();
        this.redis = new ReactiveRedisTemplate<>(factory, RedisSerializationContext.<String, byte[]>newSerializationContext()
                .key(RedisSerializer.string())
                .hashKey(RedisSerializer.string())
                .value(RedisSerializer.byteArray())
                .hashValue(RedisSerializer.byteArray())
                .build());
        this.clusterManager = clusterManager;
        startup();
    }

    public void startup() {
        if (started) {
            return;
        }
        started = true;
        clusterManager
                .getHaManager()
                .getAllNode()
                .forEach(node -> {
                    if (!node.getId().equals(clusterManager.getCurrentServerId())) {
                        handleServerNodeJoin(node);
                        handleRemoteConnection(clusterManager.getCurrentServerId(), node.getId());
                    }
                });
        disposable.add(clusterManager.getHaManager()
                                     .subscribeServerOnline()
                                     .subscribe(node -> {
                                         handleServerNodeJoin(node);
                                         handleRemoteConnection(clusterManager.getCurrentServerId(), node.getId());
                                     }));

        disposable.add(clusterManager.getHaManager()
                                     .subscribeServerOffline()
                                     .subscribe(this::handleServerNodeLeave));
    }

    public void shutdown() {
        for (ClusterConnecting value : connections.values()) {
            value.disposable.dispose();
        }
        disposable.dispose();
    }

    protected void handleServerNodeJoin(ServerNode node) {

    }

    protected void handleServerNodeLeave(ServerNode node) {

    }

    protected void handleRemoteConnection(String localId, String remoteId) {
        connections
                .computeIfAbsent(remoteId, _id -> {
                    log.debug("handle redis connection:{}", remoteId);
                    ClusterConnecting connection = new ClusterConnecting(localId, _id);
//                    sink.next(onConnectionCreated(connection));
                    eventConnectionMany.tryEmitNext(onConnectionCreated(connection));
                    return connection;
                });
    }

    protected ClusterConnecting onConnectionCreated(ClusterConnecting clusterConnecting) {
        return clusterConnecting;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Flux<EventConnection> accept() {
        return Flux.concat(Flux.fromIterable(connections.values()), eventConnectionMany.asFlux()).distinct();
    }

    protected abstract Flux<TopicPayload> listen(String localId, String brokerId);


    protected abstract Mono<Void> dispatch(String localId, String brokerId, TopicPayload payload);


    class ClusterConnecting implements EventProducer, EventConsumer {

        @Getter
        private final String brokerId;
        private final String localId;

        private final Sinks.Many<TopicPayload> inputSinkMany = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE, false);

        Sinks.Many<Subscription> subscriptionMany = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE, false);

        Sinks.Many<Subscription> unsubscriptionMany = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE, false);

        Composite disposable = Disposables.composite();

        Sinks.Many<TopicPayload> outputSinkMany = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE, false);

        private final String allSubsInfoKey;

        public ClusterConnecting(String localId, String brokerId) {
            this.brokerId = brokerId;
            this.localId = localId;
            //本地->其他节点的订阅信息
            allSubsInfoKey = "/broker/" + localId + "/" + brokerId + "/subs";

            disposable.add(subscriptionMany::tryEmitComplete);
            disposable.add(unsubscriptionMany::tryEmitComplete);
            disposable.add(inputSinkMany::tryEmitComplete);

            disposable.add(listen(localId, brokerId)
                                   .doOnNext(msg -> {
                                       if (inputSinkMany.currentSubscriberCount() == 0) {
                                           msg.release();
                                           return;
                                       }
                                       log.trace("{} handle cluster [{}] event {}", localId, brokerId, msg.getTopic());
                                       inputSinkMany.tryEmitNext(msg);
                                   })
                                   .onErrorContinue((err, res) -> log.error(err.getMessage(), err))
                                   .subscribe());


            disposable.add(redis
                                   .listenToPattern("/broker/" + brokerId + "/" + localId + "/*")
                                   .subscribe(msg -> {
                                       Subscription subscription = Payload
                                               .of(msg.getMessage())
                                               .decode(subscriptionCodec);
                                       if (subscription != null) {
                                           if (msg.getChannel().endsWith("unsub") && unsubscriptionMany.currentSubscriberCount() > 0) {
                                               unsubscriptionMany.tryEmitNext(subscription);
                                               return;
                                           }
                                           if (msg.getChannel().endsWith("sub") && subscriptionMany.currentSubscriberCount() > 0) {
                                               subscriptionMany.tryEmitNext(subscription);
                                               return;
                                           }
                                       }
                                   }));

            //加载其他节点订阅的信息
            String loadSubsInfoKey = "/broker/" + brokerId + "/" + localId + "/subs";
            disposable.add(redis
                                   .opsForSet()
                                   .members(loadSubsInfoKey)
                                   .doOnNext(msg -> {
                                       Subscription subscription = Payload.of(msg).decode(subscriptionCodec);
                                       subscriptionMany.tryEmitNext(subscription);
                                   })
                                   .onErrorContinue((err, v) -> log.warn(err.getMessage(), err))
                                   .subscribe());

            disposable.add(outputSinkMany.asFlux()
                                   .flatMap(payload -> dispatch(localId, brokerId, payload)
                                           .onErrorResume((err) -> {
                                               log.error(err.getMessage(), err);
                                               return Mono.empty();
                                           }))
                                   .onErrorContinue((err, res) -> log.error(err.getMessage(), err))
                                   .subscribe());
        }

        @Override
        public Mono<Void> subscribe(Subscription subscription) {
            byte[] sub = subscriptionCodec.encode(subscription).getBytes(true);
            String topic = "/broker/" + localId + "/" + brokerId + "/sub";

            return redis
                    .opsForSet()
                    .add(allSubsInfoKey, sub)
                    .then(redis.convertAndSend(topic, sub))
                    .then();
        }

        @Override
        public Mono<Void> unsubscribe(Subscription subscription) {
            byte[] sub = subscriptionCodec.encode(subscription).getBytes(true);
            String topic = "/broker/" + localId + "/" + brokerId + "/unsub";
            return redis
                    .opsForSet()
                    .remove(allSubsInfoKey, new Object[]{sub})
                    .then(redis.convertAndSend(topic, sub))
                    .then();
        }

        @Override
        public Flux<TopicPayload> subscribe() {
            return inputSinkMany.asFlux();
        }

        @Override
        public String getId() {
            return brokerId;
        }

        @Override
        public boolean isAlive() {
            return true;
        }

        @Override
        public void doOnDispose(Disposable disposable) {
            this.disposable.add(disposable);
        }

        @Override
        public EventBroker getBroker() {
            return AbstractClusterEventBroker.this;
        }

        @Override
        public Feature[] features() {
            return new Feature[]{Feature.consumeAnotherBroker};
        }

        @Override
        public Flux<Subscription> handleSubscribe() {
            return subscriptionMany.asFlux();
        }

        @Override
        public Flux<Subscription> handleUnSubscribe() {
            return unsubscriptionMany.asFlux();
        }

        @Override
        public Sinks.Many<TopicPayload> sinksMany() {
            return outputSinkMany;
        }

        @Override
        public void dispose() {
            disposable.dispose();
        }

        @Override
        public boolean isDisposed() {
            return false;
        }
    }

}
