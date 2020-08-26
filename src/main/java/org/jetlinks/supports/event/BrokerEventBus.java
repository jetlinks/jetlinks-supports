package org.jetlinks.supports.event;

import io.netty.util.internal.ThreadLocalRandom;
import lombok.AllArgsConstructor;
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

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private Scheduler publishScheduler = Schedulers.immediate();

    public BrokerEventBus() {
    }

    @Override
    public <T> Flux<T> subscribe(@NotNull Subscription subscription,
                                 @NotNull Decoder<T> decoder) {
        return subscribe(subscription)
                .flatMap(payload -> {
                    try {
                        payload.retain();
                        return Mono.justOrEmpty(payload.decode(decoder));
                    } finally {
                        payload.release();
                    }
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
            sink.onDispose(disposable);
            //订阅代理消息
            if (subscription.hasFeature(Subscription.Feature.broker)) {
                doSubscribeBroker(subscription)
                        .doOnSuccess(nil -> {
                            if (subscription.getDoOnSubscribe() != null) {
                                subscription.getDoOnSubscribe().run();
                            }
                        })
                        .subscribe();
                disposable.add(() -> doUnsubscribeBroker(subscription).subscribe());
            } else {
                if (subscription.getDoOnSubscribe() != null) {
                    subscription.getDoOnSubscribe().run();
                }
            }
            log.debug("local subscriber [{}],features:{},topics: {}", subscriberId, subscription.getFeatures(), subscription.getTopics());
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

    private Mono<Void> doSubscribeBroker(Subscription subscription) {
        return Flux.fromIterable(connections.values())
                .filter(conn -> conn.isProducer() && conn.isAlive())
                .cast(EventProducer.class)
                .flatMap(conn -> conn.subscribe(subscription))
                .then();

    }

    private Mono<Void> doUnsubscribeBroker(Subscription subscription) {
        return Flux.fromIterable(connections.values())
                .filter(conn -> conn.isProducer() && conn.isAlive())
                .cast(EventProducer.class)
                .flatMap(conn -> conn.unsubscribe(subscription))
                .then();
    }

    private void startBroker(EventBroker broker) {
        broker.accept()
                .subscribe(connection -> {
                    String connectionId = broker.getId().concat(":").concat(connection.getId());
                    EventConnection old = connections.put(connectionId, connection);
                    if (old == connection) {
                        return;
                    }
                    if (old != null) {
                        old.dispose();
                    }
                    connection.doOnDispose(() -> connections.remove(connectionId));

                    //从生产者订阅消息并推送到本地
                    connection
                            .asProducer()
                            .flatMap(eventProducer -> root
                                    .getAllSubscriber()
                                    .doOnNext(sub -> {
                                        for (SubscriptionInfo subscriber : sub.getSubscribers()) {
                                            if (subscriber.isLocal()) {
                                                if (subscriber.hasFeature(Subscription.Feature.broker)) {
                                                    eventProducer
                                                            .subscribe(subscriber.toSubscription(sub.getTopic()))
                                                            .subscribe();
                                                }
                                            }
                                        }
                                    })
                                    .then(Mono.just(eventProducer)))
                            .flatMapMany(EventProducer::subscribe)
                            .flatMap(payload ->
                                    doPublishFromBroker(payload, sub -> {
                                        //本地订阅的
                                        if (sub.isLocal()) {
                                            return sub.hasFeature(Subscription.Feature.broker);
                                        }
                                        if (sub.isBroker()) {
                                            //消息来自同一个broker
                                            if (sub.getEventBroker() == broker) {
                                                if (sub.getEventConnection() == connection) {
                                                    return sub.hasConnectionFeature(EventConnection.Feature.consumeSameConnection);
                                                }
                                                return sub.hasConnectionFeature(EventConnection.Feature.consumeSameBroker);
                                            }
                                            return sub.hasConnectionFeature(EventConnection.Feature.consumeAnotherBroker);
                                        }
                                        return false;

                                    }), Integer.MAX_VALUE)
                            .onErrorContinue((err, obj) -> {
                                log.error(err.getMessage(), err);
                            })
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
                                        .onErrorContinue((err, obj) -> {
                                            log.error(err.getMessage(), err);
                                        })
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
                                        .onErrorContinue((err, obj) -> {
                                            log.error(err.getMessage(), err);
                                        })
                                        .subscribe();
                            });
                });
    }

    private void handleBrokerUnsubscription(Subscription subscription, SubscriptionInfo info, EventConnection connection) {
        if (log.isDebugEnabled()) {
            log.debug("broker [{}] unsubscribe : {}", info, subscription.getTopics());
        }
        for (String topic : subscription.getTopics()) {
            AtomicBoolean unsub = new AtomicBoolean(false);
            root.append(topic)
                    .unsubscribe(sub ->
                            sub.getEventConnection() == connection
                                    && sub.getSubscriber().equals(info.getSubscriber())
                                    && unsub.compareAndSet(false, true)
                    );
        }
    }

    private void subAnotherBroker(Subscription subscription, SubscriptionInfo info, EventConnection connection) {
        //从其他broker订阅时,去掉broker标识
        //todo 还有更好到处理方式？
        Subscription sub = subscription.hasFeature(Subscription.Feature.shared)
                ? subscription.copy(Subscription.Feature.shared, Subscription.Feature.local)
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
                .flatMap(eventProducer -> eventProducer.subscribe(sub))
                .subscribe();
    }

    private void handleBrokerSubscription(Subscription subscription, SubscriptionInfo info, EventConnection connection) {
        if (log.isDebugEnabled()) {
            log.debug("broker [{}] subscribe : {}", info, subscription.getTopics());
        }
        for (String topic : subscription.getTopics()) {
            Topic<SubscriptionInfo> topic_ = root.append(topic);
            topic_.subscribe(info);
            info.onDispose(() -> topic_.unsubscribe(info));
        }
        if (subscription.hasFeature(Subscription.Feature.broker)
                && info.hasConnectionFeature(EventConnection.Feature.consumeAnotherBroker)) {
            subAnotherBroker(subscription, info, connection);
        }

    }

    private void doPublish(SubscriptionInfo info, TopicPayload payload) {
        try {
            payload.retain();
            info.sink.next(payload);
            log.debug("publish [{}] to [{}] complete", payload.getTopic(), info);
        } catch (Throwable error) {
            log.error("publish [{}] to [{}] event error", payload.getTopic(), info, error);
        } finally {
            payload.release();
        }
    }

    private Mono<Long> doPublish(String topic, Predicate<SubscriptionInfo> predicate, Function<Flux<SubscriptionInfo>, Mono<Long>> subscriberConsumer) {
        return root
                .findTopic(topic)
                .flatMapIterable(Topic::getSubscribers)
                .filter(sub -> {
                    //订阅者来自代理,但是代理已经挂了,应该自动取消订阅?
                    if (sub.isBroker() && !sub.getEventConnection().isAlive()) {
                        sub.dispose();
                        return false;
                    }
                    return predicate.test(sub);
                })
                //根据订阅者标识进行分组,以进行订阅模式判断
                .groupBy(SubscriptionInfo::getSubscriber, Integer.MAX_VALUE)
                .publishOn(publishScheduler)
                .flatMap(group -> group
                                .collectSortedList(Comparator.comparing(SubscriptionInfo::isLocal).reversed())
                                .flatMapMany(allSubs -> {
                                    SubscriptionInfo first = allSubs.get(0);
                                    if (first.hasFeature(Subscription.Feature.shared)) {
                                        //本地优先
                                        if (first.hasFeature(Subscription.Feature.local) && (first.isLocal() || allSubs.size() == 1)) {
                                            return Flux.just(first);
                                        }
                                        //随机
                                        return Flux.just(allSubs.get(ThreadLocalRandom.current().nextInt(0, allSubs.size())));
                                    }
                                    return Flux.fromIterable(allSubs);
                                }),
                        Integer.MAX_VALUE)
                // 防止多次推送给同一个消费者,
                // 比如同一个消费者订阅了: /device/1/2 和/device/1/*/
                // 推送 /device/1/2,会获取到2个相同到订阅者
                .distinct(SubscriptionInfo::getSink)
                .as(subscriberConsumer);

    }

    private Mono<Long> doPublishFromBroker(TopicPayload payload, Predicate<SubscriptionInfo> predicate) {
        return this
                .doPublish(
                        payload.getTopic(),
                        predicate,
                        flux -> flux
                                .collectList()
                                .map(subscriptionInfo -> {
                                    for (SubscriptionInfo info : subscriptionInfo) {
                                        try {
                                            payload.retain();
                                            info.sink.next(payload);
                                            if (log.isDebugEnabled()) {
                                                log.debug("broker publish [{}] to [{}] complete", payload.getTopic(), info);
                                            }
                                        } catch (Exception e) {
                                            log.warn("broker publish [{}] to [{}] error", payload.getTopic(), info, e);
                                        } finally {
                                            payload.release();
                                        }
                                    }
                                    return (long) subscriptionInfo.size();
                                })
                                .defaultIfEmpty(0L)
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
                                        .map(payload -> TopicPayload.of(topic, NativePayload.of(payload, encoder::encode)))
                                        .doOnNext(payload -> {
                                            for (SubscriptionInfo sub : subs) {
                                                doPublish(sub, payload);
                                            }
                                        })
                                        .then(Mono.just((long) subs.size()))
                                ).defaultIfEmpty(0L)
                )

                .doOnNext(subs -> {
//                    if (subs == 0) {
                    log.trace("topic [{}] has {} subscriber", topic, subs);
//                    }
                });

    }


    @AllArgsConstructor(staticName = "of")
    @Getter
    @Setter
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

        @Override
        public String toString() {
            return isLocal() ? subscriber + "@local" : subscriber + "@" + eventBroker.getId() + ":" + eventConnection.getId();
        }

        public Subscription toSubscription(String topic) {
            return Subscription.of(subscriber, new String[]{topic}, EnumDict.getByMask(Subscription.Feature.class, features).toArray(new Subscription.Feature[0]));
        }

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
