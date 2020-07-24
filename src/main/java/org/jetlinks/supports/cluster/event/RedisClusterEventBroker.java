package org.jetlinks.supports.cluster.event;

import org.jetlinks.core.codec.Codecs;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.supports.event.EventBroker;
import org.jetlinks.supports.event.EventConnection;
import org.jetlinks.supports.event.EventConsumer;
import org.jetlinks.supports.event.EventProducer;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public class RedisClusterEventBroker implements EventBroker {

    private ReactiveRedisOperations<String, byte[]> operations;

    private String id;

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Flux<EventConnection> accept() {

        return null;
    }

    class RedisEventConnection implements EventProducer, EventConsumer {

        private String brokerId;

        EmitterProcessor<TopicPayload> processor = EmitterProcessor.create(false);
        FluxSink<TopicPayload> input = processor.sink(FluxSink.OverflowStrategy.BUFFER);

        FluxSink<TopicPayload> output;

        @Override
        public Flux<Subscription> handleSubscribe() {
            return null;
        }

        @Override
        public Flux<Subscription> handleUnSubscribe() {
            return null;
        }

        @Override
        public FluxSink<TopicPayload> sink() {
            return output;
        }

        @Override
        public void subscribe(Subscription subscription) {
            operations
                    .convertAndSend("/broker/" + brokerId + "/sub", Codecs.lookup(Subscription.class).encode(subscription).bodyAsBytes())
                    .subscribe();
        }

        @Override
        public void unsubscribe(Subscription subscription) {
            operations
                    .convertAndSend("/broker/" + brokerId + "/unsub", Codecs.lookup(Subscription.class).encode(subscription).bodyAsBytes())
                    .subscribe();
        }

        @Override
        public Flux<TopicPayload> subscribe() {
            return null;
        }

        @Override
        public String getId() {
            return null;
        }

        @Override
        public boolean isAlive() {
            return false;
        }

        @Override
        public void doOnDispose(Disposable disposable) {

        }

        @Override
        public EventBroker getBroker() {
            return RedisClusterEventBroker.this;
        }
    }
}
