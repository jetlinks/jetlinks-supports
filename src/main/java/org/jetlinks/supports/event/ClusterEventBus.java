package org.jetlinks.supports.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
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
import reactor.core.scheduler.Schedulers;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClusterEventBus implements EventBus {

    private final Topic<SubscriptionInfo> root = Topic.createRoot();

    private final Map<String, EventBroker> brokers = new ConcurrentHashMap<>(32);

    private final Map<String, EventConnection> connections = new ConcurrentHashMap<>(512);

    private EventQueueManager queueManager;

    @Override
    public <T> Flux<T> subscribe(@NotNull Subscription subscription,
                                 @NotNull Decoder<T> type) {
        return subscribe(subscription)
                .flatMap(payload -> Mono.justOrEmpty(type.decode(payload)));
    }

    @Override
    public Flux<TopicPayload> subscribe(Subscription subscription) {
        return Flux.create(sink -> {
            Disposable.Composite disposable = Disposables.composite();
            for (String topic : subscription.getTopics()) {
                Topic<SubscriptionInfo> topicInfo = root.append(topic);
                SubscriptionInfo subInfo = SubscriptionInfo.of(
                        subscription.getSubscriber(),
                        EnumDict.toMask(subscription.getFeatures()),
                        sink,
                        false
                );
                topicInfo.subscribe(subInfo);
                disposable.add(() -> {
                    topicInfo.unsubscribe(subInfo);
                    subInfo.dispose();
                });
                //队列模式订阅
                if (subInfo.hasFeature(Subscription.Feature.queue)) {
                    EventQueue queue = queueManager.getQueue(topic, subInfo.subscriber);
                    disposable.add(queue.accept()
                            .doOnNext(sink::next)
                            .doOnError(sink::error)
                            .subscribe());
                }
            }
            //订阅集群消息
            if (subscription.hasFeature(Subscription.Feature.cluster)) {
                doSubscribeCluster(subscription);
                disposable.add(() -> doUnSubscribeCluster(subscription));
            }
            sink.onDispose(disposable);
        });
    }

    public void addBroker(EventBroker broker) {
        brokers.put(broker.getId(), broker);
        startBroker(broker);
    }

    private void doSubscribeCluster(Subscription subscription) {
        for (EventConnection value : connections.values()) {
            if (value.isProducer() && value.isAlive()) {
                ((EventProducer) value).subscribe(subscription);
            }
        }
    }

    private void doUnSubscribeCluster(Subscription subscription) {
        for (EventConnection value : connections.values()) {
            if (value.isProducer() && value.isAlive()) {
                ((EventProducer) value).unsubscribe(subscription);
            }
        }
    }

    private void startBroker(EventBroker broker) {
        broker.accept()
                .subscribe(connection -> {
                    String connectionId = broker.getId().concat(":").concat(connection.getId());
                    connections.put(connectionId, connection);

                    //从生产者订阅消息并推送到本地
                    connection
                            .asProducer()
                            .flatMapMany(EventProducer::subscribe)
                            .flatMap(this::doPublishLocal)
                            .subscribe();

                    //从消费者订阅获取订阅消息请求
                    connection
                            .asConsumer()
                            .subscribe(subscriber -> {
                                //接收订阅请求
                                subscriber
                                        .handleSubscribe()
                                        .doOnNext(subscription ->
                                                doSubscription(subscription,
                                                        SubscriptionInfo.of(
                                                                connectionId.concat(":").concat(subscription.getSubscriber()),
                                                                EnumDict.toMask(subscription.getFeatures()),
                                                                subscriber.sink(),
                                                                true)
                                                ))
                                        .subscribe();

                                //接收取消订阅请求
                                subscriber
                                        .handleUnSubscribe()
                                        .doOnNext(subscription ->
                                                doUnSubscription(subscription,
                                                        SubscriptionInfo.of(connectionId.concat(":").concat(subscription.getSubscriber())))
                                        )
                                        .subscribe();
                            });
                });
    }

    private void doUnSubscription(Subscription subscription, SubscriptionInfo subscriber) {
        for (String topic : subscription.getTopics()) {
            root.append(topic).unsubscribe(subscriber).forEach(SubscriptionInfo::dispose);
        }
    }

    private void doSubscription(Subscription subscription, SubscriptionInfo info) {
        for (String topic : subscription.getTopics()) {
            if(subscription.hasFeature(Subscription.Feature.queue)){
                queueManager
                        .getQueue(topic,info.getSubscriber())
                        .accept(info.sink);
            }
            root.append(topic).subscribe(info);
        }
    }

    private Mono<Long> doPublishLocal(TopicPayload payload) {
        return root
                .findTopic(payload.getTopic())
                .flatMapIterable(Topic::getSubscribers)
                .distinct(SubscriptionInfo::getSubscriber)
                .filter(sub -> sub.isLocal() && sub.hasFeature(Subscription.Feature.cluster))
                .publishOn(Schedulers.parallel())
                .flatMap(subscriptionInfo -> {
                    //队列模式,推送到队列中
                    if (subscriptionInfo.hasFeature(Subscription.Feature.queue)) {
                        return queueManager
                                .getQueue(payload.getTopic(), subscriptionInfo.getSubscriber())
                                .add(payload)
                                .then(Mono.just(1));
                    }
                    subscriptionInfo.sink.next(payload);
                    return Mono.just(1);
                })
                .onErrorContinue((err, v) -> log.error("publish {} error", payload.getTopic(), err))
                .count();
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
                    return Flux
                            .fromIterable(subs)
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
                    .add(eventStream);
        }
        return eventStream
                .doOnNext(sub.sink::next)
                .then();
    }

    @AllArgsConstructor(staticName = "of")
    @Getter
    @Setter
    @EqualsAndHashCode(of = "subscriber")
    private static class SubscriptionInfo {
        String subscriber;
        long features;
        FluxSink<TopicPayload> sink;
        boolean remote;
        Disposable.Composite disposable;

        public static SubscriptionInfo of(String subscriber) {
            return of(subscriber, 0, null, false);
        }

        public static SubscriptionInfo of(Subscription subscription,
                                          FluxSink<TopicPayload> sink,
                                          boolean remote) {
            return of(subscription.getSubscriber(),
                    EnumDict.toMask(subscription.getFeatures()),
                    sink,
                    remote);
        }

        public static SubscriptionInfo of(String subscriber,
                                          long features,
                                          FluxSink<TopicPayload> sink,
                                          boolean remote) {
            return new SubscriptionInfo(subscriber, features, sink, remote);
        }

        public SubscriptionInfo(String subscriber,
                                long features,
                                FluxSink<TopicPayload> sink,
                                boolean remote) {
            this.subscriber = subscriber;
            this.features = features;
            this.sink = sink;
            this.remote = remote;
        }

        synchronized void onDispose(Disposable disposable) {
            if (this.disposable == null) {
                this.disposable = Disposables.composite(disposable);
            } else {
                this.disposable.add(disposable);
            }
        }

        void dispose() {
            if (disposable != null) {
                disposable.dispose();
            }
        }

        boolean isLocal() {
            return !remote;
        }

        boolean hasFeature(Subscription.Feature feature) {
            return feature.in(feature);
        }
    }
}
