package org.jetlinks.supports.event;

import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.event.Cancelable;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.topic.Topic;
import static org.jetlinks.supports.event.InternalEventBus.*;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
class LocalSubscriber implements BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>>, Cancelable {
    private static final Object DISPOSED = new Object();

    private final int hashCode = System.identityHashCode(this);

    final BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> handler;
    final Function<TopicPayload, Mono<Void>> handler0;

    private final InternalEventBus parent;

    static final AtomicReferenceFieldUpdater<LocalSubscriber, Object>
        SUBSCRIPTION = AtomicReferenceFieldUpdater.newUpdater(LocalSubscriber.class, Object.class, "subscription");
    private volatile Object subscription;

    LocalSubscriber(InternalEventBus eventBus,
                    Subscription subscription,
                    BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> handler) {
        this.parent = eventBus;
        this.handler = handler;
        this.handler0 = null;
        init(parent.root, subscription);
    }

    LocalSubscriber(InternalEventBus eventBus,
                    Subscription subscription,
                    Function<TopicPayload, Mono<Void>> handler) {
        this.parent = eventBus;
        this.handler = null;
        this.handler0 = handler;
        init(parent.root, subscription);
    }

    private void init(Topic<SubscriptionInfo> root,
                      Subscription subscription) {
        String[] topics = subscription.getTopics();
        for (int i = 0, len = topics.length; i < len; i++) {
            String topic = topics[i];
            Topic<SubscriptionInfo> tTopic = root.append(topic);
            SubscriptionInfo subscriptionInfo = SubscriptionInfo
                .of(subscription,
                    tTopic,
                    this);

            log.debug("subscribe: {}", subscriptionInfo);
            if (len == 1) {
                this.subscription = subscriptionInfo;
            } else {
                if (this.subscription == null) {
                    this.subscription = new SubscriptionInfo[len];
                }
                Array.set(this.subscription, i, subscriptionInfo);
            }
            tTopic.subscribe(subscriptionInfo);

            if (subscriptionInfo.hasFeature(Subscription.Feature.broker)) {
                parent.subscribeCluster(subscriptionInfo);
            }
        }
    }

    @Override
    public void cancel() {
         // todo 取消订阅
        dispose();
    }

    //订阅收到数据时执行
    @Override
    public Mono<Void> apply(SubscriptionInfo info, TopicPayload payload) {
        return new LocalSubscriberMono(info, payload, handler, handler0);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LocalSubscriber) {
            return this == o;
        }
        return false;
    }

    @Override
    public boolean isDisposed() {
        return SUBSCRIPTION.get(this) == DISPOSED;
    }

    @Override
    public void dispose() {
        Object sub = SUBSCRIPTION.getAndSet(this, DISPOSED);
        if (sub == DISPOSED) {
            return;
        }
        if (sub instanceof SubscriptionInfo) {
            unsub((SubscriptionInfo) sub);
            return;
        }
        if (sub instanceof SubscriptionInfo[]) {
            SubscriptionInfo[] infos = (SubscriptionInfo[]) sub;
            for (SubscriptionInfo info : infos) {
                unsub(info);
            }
        }
    }

    private void unsub(SubscriptionInfo info) {
        if (info.topicRef != null) {
            info.topicRef.unsubscribe(info);
        }
        if (info.hasFeature(Subscription.Feature.broker)) {
            parent.unsubscribeCluster(info);
        }
    }

    private static class LocalSubscriberMono extends Mono<Void> {
        private final SubscriptionInfo info;
        private final TopicPayload payload;
        private final BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> handler;
        private final Function<TopicPayload, Mono<Void>> handler0;

        protected LocalSubscriberMono(SubscriptionInfo info,
                                      TopicPayload payload,
                                      BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> handler,
                                      Function<TopicPayload, Mono<Void>> handler0) {
            this.info = info;
            this.payload = payload;
            this.handler = handler;
            this.handler0 = handler0;
        }

        @Override
        public void subscribe(@Nonnull CoreSubscriber<? super Void> actual) {
            try {
                if (handler0 != null) {
                    handler0.apply(payload).subscribe(new Subscriber(actual, info, payload));
                } else if (handler != null) {
                    handler.apply(info, payload).subscribe(new Subscriber(actual, info, payload));
                }
            } catch (Throwable throwable) {
                info.error(throwable);
                log.warn("handle publish [{}] error", payload.getTopic(), throwable);
                Operators.complete(actual);
            }
        }

        @AllArgsConstructor
        private static class Subscriber extends BaseSubscriber<Void> {
            private final CoreSubscriber<? super Void> actual;
            private final SubscriptionInfo info;
            private final TopicPayload payload;

            @Override
            protected void hookOnSubscribe(@Nonnull org.reactivestreams.Subscription subscription) {
                info.in();
                actual.onSubscribe(this);
            }

            @Override
            @Nonnull
            public Context currentContext() {
                return actual.currentContext();
            }

            @Override
            protected void hookFinally(@Nonnull SignalType type) {
                actual.onComplete();
                info.out();
            }

            @Override
            protected void hookOnError(@Nonnull Throwable throwable) {
                info.error(throwable);
                log.warn("handle publish [{}] error", payload.getTopic(), throwable);
            }
        }
    }

}