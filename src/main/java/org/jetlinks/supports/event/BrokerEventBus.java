package org.jetlinks.supports.event;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.hswebframework.web.dict.EnumDict;
import org.jetlinks.core.NativePayload;
import org.jetlinks.core.codec.Codecs;
import org.jetlinks.core.codec.Decoder;
import org.jetlinks.core.codec.Encoder;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.topic.Topic;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * 支持事件代理的事件总线,可通过代理来实现集群和分布式事件总线
 *
 * @author zhouhao
 * @see EventBroker
 * @since 1.1.1
 */
@Slf4j
public class BrokerEventBus implements EventBus {

    private final Topic<SubscriptionInfo> root = Topic.createRoot();

    private final Map<String, EventBroker> brokers = new ConcurrentHashMap<>(32);

    private final Map<String, EventConnection> connections = new ConcurrentHashMap<>(512);

    @Setter
    private Scheduler publishScheduler = Schedulers.parallel();

    public BrokerEventBus() {
    }

    @Override
    @SuppressWarnings("all")
    public <T> Flux<T> subscribe(@NotNull Subscription subscription,
                                 @NotNull Decoder<T> decoder) {
        return subscribe(subscription)
                .flatMap(payload -> {
                    if (payload.getPayload() instanceof NativePayload) {
                        try {
                            Object nativeObject = ((NativePayload) payload.getPayload()).getNativeObject();
                            if (decoder.isDecodeFrom(nativeObject)) {
                                return Mono.justOrEmpty((T) nativeObject);
                            }
                        } catch (ClassCastException ignore) {
                        }
                    }
                    return Mono.justOrEmpty(decoder.decode(payload));
                });
    }

    @Override
    public Flux<TopicPayload> subscribe(Subscription subscription) {
        return Flux.create(sink -> {
            Disposable.Composite disposable = Disposables.composite();
            String subscriberId = subscription.getSubscriber();
            for (String topic : subscription.getTopics()) {
                Topic<SubscriptionInfo> topicInfo = root.append(topic);
                SubscriptionInfo subInfo = SubscriptionInfo.of(
                        subscriberId,
                        EnumDict.toMask(subscription.getFeatures()),
                        sink,
                        false
                );
                topicInfo.subscribe(subInfo);
                disposable.add(() -> {
                    topicInfo.unsubscribe(subInfo);
                    subInfo.dispose();
                });
            }
            log.debug("local subscriber [{}],features:{},topics: {}", subscriberId, subscription.getFeatures(), subscription.getTopics());
            //订阅代理消息
            if (subscription.hasFeature(Subscription.Feature.broker)) {
                doSubscribeBroker(subscription);
                disposable.add(() -> doUnsubscribeBroker(subscription));
            }
            sink.onDispose(disposable);
        });
    }

    public void addBroker(EventBroker broker) {
        brokers.put(broker.getId(), broker);
        startBroker(broker);
    }

    public void removeBroker(EventBroker broker) {
        brokers.remove(broker.getId());
    }

    public void removeBroker(String broker) {
        brokers.remove(broker);
    }

    public List<EventBroker> getBrokers() {
        return new ArrayList<>(brokers.values());
    }

    private void doSubscribeBroker(Subscription subscription) {
        for (EventConnection value : connections.values()) {
            if (value.isProducer() && value.isAlive()) {
                ((EventProducer) value).subscribe(subscription);
            }
        }
    }

    private void doUnsubscribeBroker(Subscription subscription) {
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

                    connection.doOnDispose(() -> connections.remove(connectionId));

                    //从生产者订阅消息并推送到本地
                    connection
                            .asProducer()
                            .flatMapMany(EventProducer::subscribe)
                            .flatMap(payload ->
                                    doPublishFromBroker(payload, sub -> {
                                        //本地订阅的
                                        if (sub.isLocal() && sub.hasFeature(Subscription.Feature.broker)) {
                                            return true;
                                        }
                                        if (sub.isBroker()) {
                                            //消息来自同一个broker
                                            if (sub.getEventBroker() == broker) {
                                                if (sub.getEventConnection() == connection) {
                                                    return sub.hasConnectionFeature(EventConnection.Feature.consumeSameConnection);
                                                }
                                                return sub.hasConnectionFeature(EventConnection.Feature.consumeSameBroker);
                                            }
                                            return true;
                                        }
                                        return false;

                                    }), Integer.MAX_VALUE)
                            .subscribe();

                    //从消费者订阅获取订阅消息请求
                    connection
                            .asConsumer()
                            .subscribe(subscriber -> {
                                //接收订阅请求
                                subscriber
                                        .handleSubscribe()
                                        .doOnNext(subscription ->
                                                handleBrokerSubscription(
                                                        subscription,
                                                        SubscriptionInfo.of(
                                                                subscription.getSubscriber(),
                                                                EnumDict.toMask(subscription.getFeatures()),
                                                                subscriber.sink(),
                                                                true)
                                                                .connection(broker, connection),
                                                        connection
                                                ))
                                        .subscribe();

                                //接收取消订阅请求
                                subscriber
                                        .handleUnSubscribe()
                                        .doOnNext(subscription ->
                                                handleBrokerUnsubscription(
                                                        subscription,
                                                        SubscriptionInfo.of(subscription.getSubscriber()),
                                                        connection
                                                ))
                                        .subscribe();
                            });
                });
    }

    private void handleBrokerUnsubscription(Subscription subscription, SubscriptionInfo info, EventConnection connection) {
        if (log.isDebugEnabled()) {
            log.debug("broker [{}] unsubscribe : {}", info.subscriber, subscription.getTopics());
        }
        for (String topic : subscription.getTopics()) {
            root.append(topic).unsubscribe(info).forEach(SubscriptionInfo::dispose);
        }
    }

    private void handleBrokerSubscription(Subscription subscription, SubscriptionInfo info, EventConnection connection) {
        if (log.isDebugEnabled()) {
            log.debug("broker [{}] subscribe : {}", info.subscriber, subscription.getTopics());
        }
        for (String topic : subscription.getTopics()) {
            root.append(topic).subscribe(info);
        }
        if (!info.hasConnectionFeature(EventConnection.Feature.consumeAnotherBroker)) {
            return;
        }

        //从其他broker订阅
        Subscription sub = subscription.hasFeature(Subscription.Feature.queue)
                ? subscription.copy(Subscription.Feature.queue, Subscription.Feature.local)
                : subscription.copy(Subscription.Feature.local);

        Flux.fromIterable(connections.values())
                .filter(conn -> {
                    if (conn == connection) {
                        return info.hasConnectionFeature(EventConnection.Feature.consumeSameConnection);
                    }
                    if (conn.getBroker() == connection.getBroker()) {
                        return info.hasConnectionFeature(EventConnection.Feature.consumeSameBroker);
                    }
                    return true;
                })
                .flatMap(EventConnection::asProducer)
                .subscribe(eventProducer -> eventProducer.subscribe(sub));
    }

    private Mono<Long> doPublish(String topic, Predicate<SubscriptionInfo> predicate, Function<Flux<SubscriptionInfo>, Mono<Long>> subscriberConsumer) {
        return root
                .findTopic(topic)
                .flatMapIterable(Topic::getSubscribers)
                .filter(sub -> {
                    if (sub.isBroker() && !sub.getEventConnection().isAlive()) {
                        return false;
                    }
                    return predicate.test(sub);
                })
                .groupBy(SubscriptionInfo::getSubscriber, Integer.MAX_VALUE)
                .publishOn(publishScheduler)
                .flatMap(group -> group
                                .index()
                                .takeUntil(tp2 -> tp2.getT1() > 0 && tp2.getT2().hasFeature(Subscription.Feature.queue))
                                .map(Tuple2::getT2),
                        Integer.MAX_VALUE)
                .as(subscriberConsumer);

    }

    private Mono<Long> doPublishFromBroker(TopicPayload payload, Predicate<SubscriptionInfo> predicate) {
        return this
                .doPublish(payload.getTopic(), predicate, flux -> flux
                        .doOnNext(subscriptionInfo -> {
                            try {
                                subscriptionInfo.sink.next(payload);
                                if (log.isDebugEnabled()) {
                                    log.debug("broker publish [{}] to [{}] complete", payload.getTopic(), subscriptionInfo.subscriber);
                                }
                            } catch (Exception e) {
                                log.warn("broker publish [{}] to [{}] error", payload.getTopic(), subscriptionInfo.subscriber, e);
                            }
                        })
                        .count()
                );
    }

    @Override
    @SuppressWarnings("all")
    public <T> Mono<Long> publish(String topic, Publisher<T> event) {
        return publish(topic, (Encoder<T>) msg -> Codecs.lookup((Class<T>) msg.getClass()).encode(msg), event);
    }

    @Override
    public <T> Mono<Long> publish(String topic, Encoder<T> encoder, Publisher<? extends T> eventStream) {
        return this
                .doPublish(topic,
                        sub -> !sub.isLocal() || sub.hasFeature(Subscription.Feature.local),
                        subscribers -> subscribers
                                .collectList()
                                .filter(CollectionUtils::isNotEmpty)
                                .flatMap(subs -> Flux
                                        .from(eventStream)
                                        .map(payload -> TopicPayload.of(topic, NativePayload.of(payload, () -> encoder.encode(payload).getBody())))
                                        .doOnNext(payload -> {
                                            for (SubscriptionInfo sub : subs) {
                                                doPublish(sub, payload);
                                            }
                                        })
                                        .then(Mono.just((long) subs.size()))
                                )
                );

    }

    private void doPublish(SubscriptionInfo info, TopicPayload payload) {
        try {
            info.sink.next(payload);
            log.debug("publish [{}] to [{}] complete", payload.getTopic(), info.subscriber);
        } catch (Throwable error) {
            log.error("publish [{}] to [{}] event error", info.subscriber, payload.getTopic(), error);
        }
    }

    @AllArgsConstructor(staticName = "of")
    @Getter
    @Setter
    @EqualsAndHashCode(of = "subscriber")
    private static class SubscriptionInfo implements Disposable {
        String subscriber;
        long features;
        FluxSink<TopicPayload> sink;
        @Getter
        boolean broker;
        Disposable.Composite disposable;

        //broker only
        EventBroker eventBroker;

        EventConnection eventConnection;

        long connectionFeatures;

        public SubscriptionInfo connection(EventBroker broker, EventConnection connection) {
            this.eventConnection = connection;
            this.eventBroker = broker;
            this.connectionFeatures = EnumDict.toMask(connection.features());
            return this;
        }

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
                                boolean broker) {
            this.subscriber = subscriber;
            this.features = features;
            this.sink = sink;
            this.broker = broker;
        }

        synchronized void onDispose(Disposable disposable) {
            if (this.disposable == null) {
                this.disposable = Disposables.composite(disposable);
            } else {
                this.disposable.add(disposable);
            }
        }

        public void dispose() {
            if (disposable != null) {
                disposable.dispose();
            }
        }

        boolean isLocal() {
            return !broker;
        }

        boolean hasFeature(Subscription.Feature feature) {
            return feature.in(this.features);
        }

        boolean hasConnectionFeature(EventConnection.Feature feature) {
            return feature.in(this.connectionFeatures);
        }
    }
}
