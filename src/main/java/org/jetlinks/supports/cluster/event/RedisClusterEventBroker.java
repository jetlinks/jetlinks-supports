package org.jetlinks.supports.cluster.event;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.Payload;
import org.jetlinks.core.cluster.ClusterManager;
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
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RedisClusterEventBroker implements EventBroker {

    private final ReactiveRedisOperations<String, byte[]> operations;


    private final String id;
    private final EmitterProcessor<EventConnection> processor = EmitterProcessor.create(false);

    private final Map<String, ClusterConnecting> connections = new ConcurrentHashMap<>();

    private final FluxSink<EventConnection> sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);

    private final ClusterManager clusterManager;

    private boolean started = false;

    public RedisClusterEventBroker(ClusterManager clusterManager, ReactiveRedisConnectionFactory factory) {
        this.id = clusterManager.getClusterName();
        this.operations = new ReactiveRedisTemplate<>(factory, RedisSerializationContext.<String, byte[]>newSerializationContext()
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
                        handleRemoteConnection(clusterManager.getCurrentServerId(), node.getId());
                    }
                });
        clusterManager.getHaManager()
                .subscribeServerOnline()
                .subscribe(node -> handleRemoteConnection(clusterManager.getCurrentServerId(), node.getId()));

//        clusterManager.getHaManager()
//                .subscribeServerOffline()
//                .subscribe(node -> Optional
//                        .ofNullable(connections.remove(node.getId()))
//                        .ifPresent(conn -> conn.disposable.dispose())
//                );
    }

    public void shutdown() {
        for (ClusterConnecting value : connections.values()) {
            value.disposable.dispose();
        }
    }

    private void handleRemoteConnection(String localId, String id) {
        connections
                .computeIfAbsent(id, _id -> {
                    log.debug("handle redis connection:{}", id);
                    ClusterConnecting connection = new ClusterConnecting(localId, _id);
                    sink.next(connection);
                    return connection;
                });
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Flux<EventConnection> accept() {
        return Flux.concat(Flux.fromIterable(connections.values()), processor).distinct();
    }

    private final Codec<Subscription> subscriptionCodec = Codecs.lookup(Subscription.class);
    private final Codec<TopicPayload> topicPayloadCodec = Codecs.lookup(TopicPayload.class);


    class ClusterConnecting implements EventProducer, EventConsumer {

        private final String brokerId;
        private final String localId;

        private final EmitterProcessor<TopicPayload> processor = EmitterProcessor.create(false);
        private final FluxSink<TopicPayload> input = processor.sink(FluxSink.OverflowStrategy.BUFFER);

        FluxSink<TopicPayload> output;

        EmitterProcessor<Subscription> subProcessor = EmitterProcessor.create(false);
        FluxSink<Subscription> subSink = subProcessor.sink(FluxSink.OverflowStrategy.BUFFER);

        EmitterProcessor<Subscription> unsubProcessor = EmitterProcessor.create(false);
        FluxSink<Subscription> unsubSink = unsubProcessor.sink(FluxSink.OverflowStrategy.BUFFER);

        Disposable.Composite disposable = Disposables.composite();

        private final String allSubsInfoKey;

        public ClusterConnecting(String localId, String brokerId) {
            this.brokerId = brokerId;
            this.localId = localId;
            //本地->其他节点的订阅信息
            allSubsInfoKey = "/broker/" + localId + "/" + brokerId + "/subs";


            disposable.add(subProcessor::onComplete);
            disposable.add(unsubProcessor::onComplete);
            disposable.add(processor::onComplete);

            disposable.add(clusterManager
                    .<byte[]>getQueue("/broker/bus/" + brokerId + "/" + localId)
                    .subscribe()
                    .doOnNext(msg -> {
                        if (!processor.hasDownstreams()) {
                            return;
                        }
                        TopicPayload payload = topicPayloadCodec.decode(Payload.of(Unpooled.wrappedBuffer(msg)));
                        log.trace("{} handle redis [{}] event {}", localId, brokerId, payload.getTopic());
                        input.next(payload);
                    })
                    .onErrorContinue((err, res) -> log.error(err.getMessage(), err))
                    .subscribe());


            disposable.add(operations
                    .listenToPattern("/broker/" + brokerId + "/" + localId + "/*")
                    .subscribe(msg -> {
                        Subscription subscription = subscriptionCodec.decode(Payload.of(Unpooled.wrappedBuffer(msg.getMessage())));
                        if (subscription != null) {
                            if (msg.getChannel().endsWith("unsub") && unsubProcessor.hasDownstreams()) {
                                unsubSink.next(subscription);
                                return;
                            }
                            if (msg.getChannel().endsWith("sub") && subProcessor.hasDownstreams()) {
                                subSink.next(subscription);
                                return;
                            }
                        }
                    }));

            //加载其他节点订阅的信息
            String loadSubsInfoKey = "/broker/" + brokerId + "/" + localId + "/subs";
            disposable.add(operations
                    .opsForSet()
                    .members(loadSubsInfoKey)
                    .doOnNext(msg -> {
                        Subscription subscription = subscriptionCodec.decode(Payload.of(Unpooled.wrappedBuffer(msg)));
                        subSink.next(subscription);
                    })
                    .onErrorContinue((err, v) -> log.warn(err.getMessage(), err))
                    .subscribe());

            disposable.add(Flux.<TopicPayload>create(sink -> this.output = sink)
                    .flatMap(payload -> {
                        payload.retain();
                        Payload encoded = topicPayloadCodec.encode(payload);
                        byte[] body = encoded.getBytes(true);
//                        return operations.convertAndSend("/broker/bus/" + localId + "/" + brokerId, body);
                        return clusterManager
                                .getQueue("/broker/bus/" + localId + "/" + brokerId)
                                .add(Mono.just(body));
                    })
                    .onErrorContinue((err, res) -> log.error(err.getMessage(), err))
                    .subscribe());
        }

        @Override
        public Mono<Void> subscribe(Subscription subscription) {
            byte[] sub = subscriptionCodec.encode(subscription).getBytes(true);
            String topic = "/broker/" + localId + "/" + brokerId + "/sub";

            return operations
                    .opsForSet()
                    .add(allSubsInfoKey, sub)
                    .then(operations.convertAndSend(topic, sub))
                    .then();
        }

        @Override
        public Mono<Void> unsubscribe(Subscription subscription) {
            byte[] sub = subscriptionCodec.encode(subscription).getBytes(true);
            String topic = "/broker/" + localId + "/" + brokerId + "/unsub";
            return operations
                    .opsForSet()
                    .remove(allSubsInfoKey, new Object[]{sub})
                    .then(operations.convertAndSend(topic, sub))
                    .then();
        }

        @Override
        public Flux<TopicPayload> subscribe() {
            return processor;
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
            return RedisClusterEventBroker.this;
        }

        @Override
        public Feature[] features() {
            return new Feature[]{Feature.consumeAnotherBroker};
        }

        @Override
        public Flux<Subscription> handleSubscribe() {
            return subProcessor;
        }

        @Override
        public Flux<Subscription> handleUnSubscribe() {
            return unsubProcessor;
        }

        @Override
        public FluxSink<TopicPayload> sink() {
            return output;
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
