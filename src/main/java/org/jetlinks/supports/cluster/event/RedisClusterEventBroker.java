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
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RedisClusterEventBroker implements EventBroker {

    private final ReactiveRedisOperations<String, byte[]> operations;


    private final String id;
    private final EmitterProcessor<EventConnection> processor = EmitterProcessor.create(false);

    private final Map<String, EventConnection> connections = new ConcurrentHashMap<>();

    private final FluxSink<EventConnection> sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);

    public RedisClusterEventBroker(ClusterManager clusterManager, ReactiveRedisConnectionFactory factory) {
        this.id = clusterManager.getClusterName();
        this.operations = new ReactiveRedisTemplate<>(factory, RedisSerializationContext.<String, byte[]>newSerializationContext()
                .key(RedisSerializer.string())
                .hashKey(RedisSerializer.string())
                .value(RedisSerializer.byteArray())
                .hashValue(RedisSerializer.byteArray())
                .build());
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
    }

    private void handleRemoteConnection(String localId, String id) {
        connections
                .computeIfAbsent(id, _id -> {
                    log.debug("handle redis connection:{}", id);
                    EventConnection connection = new ClusterConnecting(localId, _id);
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


        public ClusterConnecting(String localId, String brokerId) {
            this.brokerId = brokerId;
            this.localId = localId;
            operations
                    .listenToChannel("/broker/bus/" + brokerId + "/" + localId)
                    .doOnNext(msg -> {
                        if (!processor.hasDownstreams()) {
                            return;
                        }
                        TopicPayload payload = topicPayloadCodec.decode(Payload.of(Unpooled.wrappedBuffer(msg.getMessage())));
                        log.debug("{} handle redis [{}] event {}", localId, brokerId, payload.getTopic());
                        input.next(payload);
                    })
                    .onErrorContinue((err, res) -> log.error(err.getMessage(), err))
                    .subscribe();

            operations
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
                    });


            Flux.<TopicPayload>create(sink -> this.output = sink)
                    .flatMap(payload -> {
                        byte[] body = topicPayloadCodec.encode(payload).bodyAsBytes(true);
                        return operations.convertAndSend("/broker/bus/" + localId + "/" + brokerId, body);
                    })
                    .onErrorContinue((err, res) -> {
                        log.error(err.getMessage(), err);
                    }).subscribe();
        }

        @Override
        public void subscribe(Subscription subscription) {
            operations
                    .convertAndSend("/broker/" + localId + "/" + brokerId + "/sub", subscriptionCodec.encode(subscription).bodyAsBytes(true))
                    .subscribe();
        }

        @Override
        public void unsubscribe(Subscription subscription) {
            operations
                    .convertAndSend("/broker/" + localId + "/" + brokerId + "/unsub", subscriptionCodec.encode(subscription).bodyAsBytes(true))
                    .subscribe();
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
    }

}
