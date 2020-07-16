package org.jetlinks.supports.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.dict.EnumDict;
import org.jetlinks.core.codec.Codecs;
import org.jetlinks.core.codec.Decoder;
import org.jetlinks.core.codec.Encoder;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.topic.Topic;
import org.reactivestreams.Publisher;
import org.springframework.util.CollectionUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import javax.validation.constraints.NotNull;

@Slf4j
public class ClusterEventBus implements EventBus {

    private final Topic<SubscriptionInfo> root = Topic.createRoot();

    private EventQueueManager queueManager;

    @Override
    public <T> Flux<T> subscribe(@NotNull Subscription subscription, @NotNull Decoder<T> type) {
        return subscribe(subscription)
                .flatMap(payload -> Mono.justOrEmpty(type.decode(payload)));
    }

    @Override
    public Flux<TopicPayload> subscribe(Subscription subscription) {
        return Flux.create(sink -> {
            Disposable.Composite disposable = Disposables.composite();
            for (String topic : subscription.getTopics()) {
                Topic<SubscriptionInfo> topicInfo = root.append(topic);
                SubscriptionInfo subInfo = new SubscriptionInfo(
                        subscription.getSubscriber(),
                        EnumDict.toMask(subscription.getFeatures()),
                        sink,
                        false
                );
                topicInfo.subscribe(subInfo);
                disposable.add(() -> topicInfo.unsubscribe(subInfo));
                //队列模式订阅
                if (subInfo.hasFeature(Subscription.Feature.queue)) {
                    EventQueue queue = queueManager.getQueue(topic, subInfo.subscriber);
                    disposable.add(queue.accept()
                            .doOnNext(payload -> TopicPayload.of(topic, payload))
                            .doOnError(sink::error)
                            .subscribe());
                }
                sink.onDispose(disposable);
            }
            //订阅集群消息
            if (subscription.hasFeature(Subscription.Feature.cluster)) {
                doSubscribeCluster(subscription);
                disposable.add(() -> doUnSubscribeCluster(subscription));
            }
        });
    }

    private void doSubscribeCluster(Subscription subscription) {

    }

    private void doUnSubscribeCluster(Subscription subscription) {

    }


    @Override
    @SuppressWarnings("all")
    public <T> Mono<Integer> publish(String topic, Publisher<T> event) {
        return publish(topic, (Encoder<T>) msg -> Codecs.lookup((Class<T>) msg.getClass()).encode(msg), event);
    }

    @Override
    public <T> Mono<Integer> publish(String topic, Encoder<T> encoder, Publisher<? extends T> eventStream) {
        return root.findTopic(topic)
                .flatMapIterable(Topic::getSubscribers)
                .distinct(SubscriptionInfo::getSubscriber)
                .collectList()
                .filter(list -> !CollectionUtils.isEmpty(list))
                .flatMap(subs -> {
                    Flux<TopicPayload> payloadStream = Flux.from(eventStream)
                            .map(payload -> TopicPayload.of(topic, encoder.encode(payload)))
                            .publish()
                            .refCount(subs.size());
                    return Flux.fromIterable(subs)
                            .flatMap(subInfo -> this
                                    .doPublish(topic, subInfo, payloadStream)
                                    .onErrorResume(error -> {
                                        log.error("publish [{}@{}] event error", subInfo.subscriber, topic, error);
                                        return Mono.empty();
                                    }))
                            .then(Mono.just(subs.size()));
                })
                .defaultIfEmpty(0);
    }

    private Mono<Void> doPublish(String topic, SubscriptionInfo sub, Flux<TopicPayload> eventStream) {
        //本地订阅者，但是不订阅本地推送的消息
        if (!sub.isRemote() && !sub.hasFeature(Subscription.Feature.local)) {
            return Mono.empty();
        }
        //队列模式
        if (sub.hasFeature(Subscription.Feature.queue)) {
            return queueManager
                    .getQueue(topic, sub.subscriber)
                    .add(eventStream)
                    ;
        }
        return eventStream
                .doOnNext(sub.sink::next)
                .then();
    }

    @AllArgsConstructor
    @Getter
    @Setter
    private static class SubscriptionInfo {
        String subscriber;
        long features;
        FluxSink<TopicPayload> sink;
        boolean remote;

        boolean hasFeature(Subscription.Feature feature) {
            return feature.in(feature);
        }
    }
}
