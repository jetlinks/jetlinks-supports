package org.jetlinks.supports.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.ThreadLocalRandom;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.dict.EnumDict;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.Decoder;
import org.jetlinks.core.codec.Encoder;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.topic.Topic;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.core.utils.SerializeUtils;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.function.Function4;
import reactor.util.context.ContextView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class InternalEventBus implements EventBus {

    protected final Topic<SubscriptionInfo> subscriptionTable = Topic.createRoot();


    @Override
    public Flux<TopicPayload> subscribe(Subscription subscription) {
        return Flux
            .create(sink -> sink
                .onDispose(
                    subscribe(subscription, e -> {
                        sink.next(e);
                        return Mono.empty();
                    })
                ));
    }

    @Override
    public <T> Flux<T> subscribe(Subscription subscription, Decoder<T> decoder) {
        return this
            .subscribe(subscription)
            .mapNotNull(payload -> {
                try {
                    //收到消息后解码
                    return payload.decode(decoder, false);
                } catch (Throwable e) {
                    //忽略解码错误,如果返回错误,可能会导致整个流中断
                    log.error("decode message [{}] error", payload.getTopic(), e);
                } finally {
                    ReferenceCountUtil.safeRelease(payload);
                }
                return null;
            });
    }

    @Override
    public <T> Flux<T> subscribe(Subscription subscription, Class<T> type) {
        return this
            .subscribe(subscription)
            .mapNotNull(payload -> {
                try {
                    return payload.decode(type);
                } catch (Throwable e) {
                    log.error("decode message [{}] error", payload.getTopic(), e);
                }
                return null;
            });
    }

    public Disposable subscribe(Subscription subscription,
                                Function<TopicPayload, Mono<Void>> handler) {

        Disposable.Composite disposable = Disposables.composite();

        Function<TopicPayload, Mono<Void>> lazyHandler = new LocalHandler(handler);

        for (String topic : subscription.getTopics()) {
            SubscriptionInfo subscriptionInfo = SubscriptionInfo
                .of(subscription,
                    topic,
                    lazyHandler,
                    false);
            log.debug("subscribe: {}", subscriptionInfo);

            Topic<SubscriptionInfo> tTopic = subscriptionTable.append(topic);

            tTopic.subscribe(subscriptionInfo);

            disposable.add(() -> {
                log.debug("unsubscribe: {}", subscriptionInfo);
                tTopic.unsubscribe(subscriptionInfo);
                subscriptionInfo.dispose();
            });

            //订阅集群
            if (subscriptionInfo.hasFeature(Subscription.Feature.broker)) {
                disposable.add(subscribeToCluster(subscriptionInfo));
            }

        }

        return disposable;
    }

    protected Disposable subscribeToCluster(SubscriptionInfo info) {
        return Disposables.disposed();
    }

    @Override
    public <T> Mono<Long> publish(String topic, Publisher<T> event) {

        return doPublish(
            topic,
            event,
            (t, e, p, f) -> Flux
                .from(e)
                .flatMap(val -> publishFromLocal(t, val, p, f))
                .then(),
            sub -> sub.isCluster() || sub.hasFeature(Subscription.Feature.local));
    }

    @Override
    public <T> Mono<Long> publish(String topic, T event, Scheduler scheduler) {
        return this
            .publish(topic, event)
            .subscribeOn(scheduler);
    }

    @Override
    public <T> Mono<Long> publish(String topic, Encoder<T> encoder, T event) {
        return publish(topic, event);
    }

    @Override
    public <T> Mono<Long> publish(String topic, T event) {
        return doPublish(topic,
                         event,
                         this::publishFromLocal,
                         sub -> sub.isCluster() || sub.hasFeature(Subscription.Feature.local));
    }

    protected Mono<Void> doPublish0(String topic, TopicPayload payload, List<SubscriptionInfo> subs, ContextView ctx) {

        int subSize = subs.size();

        //只有一个订阅者时,不需要排序,减少内存占用
        TreeMap<Integer, ArrayList<Mono<Void>>> priority = subSize == 1 ? null : new TreeMap<>();

        Mono<Void> task = null;
        Function<TopicPayload, Mono<Void>> handler;

        for (SubscriptionInfo sub : subs) {
            log.trace("publish {} to {}", topic, sub);
            handler = sub.handler;
            task = handler.apply(payload);
            if (subSize > 1) {
                priority
                    .computeIfAbsent(sub.priority, ignore -> new ArrayList<>(subSize))
                    .add(task);
            }
        }

        if (task == null) {
            return Mono.empty();
        }

        if (subSize == 1) {
            return task;
        }

        if (priority.size() == 1) {
            return Flux
                .merge(priority.get(subs.get(0).priority))
                .then();
        }

        //不同优先级之间串行,同一个优先级并行
        return Flux
            .fromIterable(priority.values())
            .concatMap(Flux::merge)
            .then();
    }

    private <T> Mono<Void> publishFromLocal(String topic, T value, List<SubscriptionInfo> subs, ContextView ctx) {
        TopicPayload payload = TopicPayload.of(topic, Payload.of(value, null));

        TraceHolder.writeContextTo(ctx, payload, TopicPayload::addHeader);

        return doPublish0(topic, payload, subs, ctx);
    }

    @Override
    public <T> Mono<Long> publish(String topic,
                                  Encoder<T> encoder,
                                  Publisher<? extends T> eventStream) {
        return publish(topic, eventStream);
    }

    @Override
    public <T> Mono<Long> publish(String topic,
                                  Encoder<T> encoder,
                                  Publisher<? extends T> eventStream,
                                  Scheduler scheduler) {
        return publish(topic, eventStream);
    }

    private static final FastThreadLocal<Set<SubscriptionInfo>> PUB_HANDLERS = new FastThreadLocal<Set<SubscriptionInfo>>() {
        @Override
        protected Set<SubscriptionInfo> initialValue() {
            return new HashSet<>();
        }
    };

    private static final FastThreadLocal<Set<Object>> DISTINCT_HANDLERS = new FastThreadLocal<Set<Object>>() {
        @Override
        protected Set<Object> initialValue() {
            return new HashSet<>();
        }
    };

    private static final FastThreadLocal<Map<String, List<SubscriptionInfo>>> SHARED = new FastThreadLocal<Map<String, List<SubscriptionInfo>>>() {
        @Override
        protected Map<String, List<SubscriptionInfo>> initialValue() {
            return new HashMap<>();
        }
    };

    private <T> Mono<Long> doPublish(String topic,
                                     T arg,
                                     Function4<String, T, List<SubscriptionInfo>, ContextView, Mono<Void>> handler,
                                     Predicate<SubscriptionInfo> predicate) {

        //共享订阅,只有一个订阅者能收到
        Map<String, List<SubscriptionInfo>> sharedMap = SHARED.get();
        //去重
        Set<Object> distinct = DISTINCT_HANDLERS.get();
        //订阅者信息
        Set<SubscriptionInfo> readyToPub = PUB_HANDLERS.get();
        Mono<Long> task;
        try {
            //从订阅表中查找topic
            subscriptionTable
                .findTopic(topic, predicate, sharedMap, distinct, readyToPub,
                           (_predicate, _sharedMap, _distinct, _readyToPub, subs) -> {

                               Set<SubscriptionInfo> subscriptions = subs.getSubscribers();
                               if (subscriptions.isEmpty()) {
                                   return;
                               }

                               for (SubscriptionInfo sub : subscriptions) {
                                   if (!_predicate.test(sub) || !_distinct.add(sub.handler)) {
                                       continue;
                                   }
                                   //共享订阅时,添加到缓存,最后再处理
                                   if (sub.hasFeature(Subscription.Feature.shared)) {
                                       _sharedMap
                                           .computeIfAbsent(sub.subscriber, ignore -> new ArrayList<>(8))
                                           .add(sub);
                                       continue;
                                   }
                                   _readyToPub.add(sub);
                               }
                           },
                           (_predicate, _sharedMap, _distinct, _readyToPub) -> {

                               if (_sharedMap.isEmpty()) {
                                   return;
                               }
                               //处理共享订阅
                               ALL:
                               for (List<SubscriptionInfo> value : _sharedMap.values()) {
                                   int size = value.size();
                                   if (size == 0) {
                                       continue;
                                   }
                                   SubscriptionInfo first = value.get(0);
                                   //只有一个订阅者,直接推送
                                   if (size == 1) {
                                       _readyToPub.add(first);
                                       continue;
                                   }
                                   //优先本地
                                   if (first.hasFeature(Subscription.Feature.sharedLocalFirst)) {
                                       for (SubscriptionInfo subscriptionInfo : value) {
                                           if (!subscriptionInfo.isCluster()) {
                                               _readyToPub.add(subscriptionInfo);
                                               continue ALL;
                                           }
                                       }
                                   }
                                   //最先订阅的收到数据
                                   if (first.hasFeature(Subscription.Feature.sharedOldest)) {
                                       //按订阅时间排序取第一个
                                       value.sort(SubscriptionInfo.comparatorByTime);
                                       _readyToPub.add(value.get(0));

                                       continue;
                                   }
                                   // 默认随机
                                   _readyToPub.add(value.get(ThreadLocalRandom
                                                                 .current()
                                                                 .nextInt(0, size)));
                               }

                           });

            if (readyToPub.isEmpty()) {
                return Reactors.ALWAYS_ZERO_LONG;
            }

            List<SubscriptionInfo> pub = new ArrayList<>(readyToPub);

            task = Mono
                .deferContextual(ctx -> handler.apply(topic, arg, pub, ctx))
                .thenReturn((long) pub.size());

        } finally {
            sharedMap.clear();
            distinct.clear();
            readyToPub.clear();
        }
        return task;
    }

    private static class LocalHandler implements Function<TopicPayload, Mono<Void>> {
        private final int hashCode = Objects.hashCode(this);
        private final Function<TopicPayload, Mono<Void>> handler;

        private LocalHandler(Function<TopicPayload, Mono<Void>> handler) {
            this.handler = handler;
        }

        @Override
        public Mono<Void> apply(TopicPayload payload) {
            try {
                return handler
                    .apply(payload)
                    .onErrorResume(err -> {
                        log.warn("handle publish [{}] error", payload.getTopic(), err);
                        return Mono.empty();
                    });
            } catch (Throwable err) {
                log.warn("handle publish [{}] error", payload.getTopic(), err);
                return Mono.empty();
            }
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }


    @EqualsAndHashCode(
        of = {"time", "subscriber", "topic", "cluster", "clusterServerId"},
        cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SubscriptionInfo implements Disposable, Externalizable {
        static Comparator<SubscriptionInfo> comparatorByTime = Comparator.comparingLong(SubscriptionInfo::getTime);
        static Comparator<SubscriptionInfo> comparatorPriority = Comparator.comparingLong(SubscriptionInfo::getPriority);
        private long time;

        private String subscriber;

        private String topic;

        private boolean cluster;

        private String clusterServerId;

        private long features;

        private transient Function<TopicPayload, Mono<Void>> handler;

        private int priority;


        public static SubscriptionInfo of(Subscription subscription,
                                          String topic,
                                          Function<TopicPayload, Mono<Void>> handler,
                                          boolean cluster) {
            SubscriptionInfo info = new SubscriptionInfo();
            info.time = System.currentTimeMillis();
            info.topic = topic;
            info.subscriber = subscription.getSubscriber();

            info.handler = handler;

            info.features = EnumDict.toMask(subscription.getFeatures());
            info.cluster = cluster;
            info.priority = subscription.getPriority();
            return info;
        }

        @Override
        public void dispose() {

        }

        @Override
        @JsonIgnore
        public boolean isDisposed() {
            return Disposable.super.isDisposed();
        }

        boolean hasFeature(Subscription.Feature feature) {
            return feature.in(this.features);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(this.time);
            out.writeUTF(this.subscriber);
            out.writeUTF(this.topic);
            out.writeLong(this.features);
            out.writeBoolean(this.cluster);
            SerializeUtils.writeObject(clusterServerId, out);
            out.writeInt(priority);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.time = in.readLong();
            this.subscriber = in.readUTF();
            this.topic = in.readUTF();
            this.features = in.readLong();
            this.cluster = in.readBoolean();
            this.clusterServerId = (String) SerializeUtils.readObject(in);
            this.priority = in.readInt();
        }

        @Override
        public String toString() {
            return subscriber + "@" + (clusterServerId == null ? "local" : clusterServerId) + "::" + topic;
        }
    }
}
