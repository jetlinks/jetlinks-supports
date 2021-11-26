package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.ClusterTopic;
import org.reactivestreams.Publisher;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.util.concurrent.atomic.AtomicBoolean;

public class RedisClusterTopic<T> implements ClusterTopic<T> {

    private final String topicName;

    private final ReactiveRedisOperations<Object, T> operations;

    private final Sinks.Many<TopicMessage<T>> topicMessageMany;

    private final AtomicBoolean subscribed = new AtomicBoolean();

    public RedisClusterTopic(String topic, ReactiveRedisOperations<Object, T> operations) {
        this.topicName = topic;
        this.operations = operations;
        topicMessageMany = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);
    }

    private Disposable disposable;

    private void doSubscribe() {
        if (subscribed.compareAndSet(false, true)) {
            disposable = operations
                    .listenToPattern(topicName)
                    .subscribe(data -> {
                        if (topicMessageMany.currentSubscriberCount() == 0) {
                            disposable.dispose();
                            subscribed.compareAndSet(true, false);
                        } else {
                            topicMessageMany.tryEmitNext(new TopicMessage<T>() {
                                @Override
                                public String getTopic() {
                                    return data.getChannel();
                                }

                                @Override
                                public T getMessage() {
                                    return data.getMessage();
                                }
                            });
                        }
                    });
        }
    }

    @Override
    public Flux<TopicMessage<T>> subscribePattern() {
        return topicMessageMany
                .asFlux()
                .doOnSubscribe((r) -> doSubscribe());
    }

    @Override
    public Mono<Integer> publish(Publisher<? extends T> publisher) {
        return Flux.from(publisher)
                .flatMap(data -> operations.convertAndSend(topicName, data))
                .last(1L)
                .map(Number::intValue);
    }
}
