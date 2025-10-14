package org.jetlinks.supports.event;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.ThreadLocalRandom;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.dict.EnumDict;
import org.jetlinks.core.event.Cancelable;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.lang.SeparatedCharSequence;
import org.jetlinks.core.lang.SeparatedString;
import org.jetlinks.core.lang.SharedPathString;
import org.jetlinks.core.message.HeaderKey;
import org.jetlinks.core.metadata.Jsonable;
import org.jetlinks.core.topic.Topic;
import org.jetlinks.core.topic.TopicView;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.RecyclerUtils;
import org.jetlinks.core.utils.json.ObjectMappers;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.function.Function5;
import reactor.util.context.ContextView;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class InternalEventBus implements EventBus {

    static final HeaderKey<Map<CharSequence, String>> SHARE_PUBLISHED = HeaderKey.of("_spd", Collections.emptyMap());

    private final static FastThreadLocal<Set<Object>> DISTINCT =
        new FastThreadLocal<Set<Object>>() {
            @Override
            protected Set<Object> initialValue() {
                return new HashSet<>();
            }
        };

    final Topic<SubscriptionInfo> root = Topic.createRoot();

    private final Disposable.Composite disposable = Disposables.composite();

    @Setter
    @Getter
    //单个订阅的最大缓冲区大小
    int maxBufferSize = 100_0000;

    public InternalEventBus() {
        init();
    }

    public Topic<SubscriptionInfo> getSubscriptionTable() {
        return root;
    }

    private void init() {

        //每间隔5分钟 清理一次
        disposable.add(
            Flux.interval(Duration.ofMinutes(5))
                .onBackpressureDrop()
                .doOnNext(ignore -> cleanup())
                .subscribe()
        );

        registerMBean();
    }

    public void shutdown() {
        log.info("shutdown eventbus");
        disposable.dispose();
        unregisterMBean();
    }

    void cleanup() {
        root.cleanup();

    }

    @Override
    public Flux<TopicPayload> subscribe(Subscription subscription) {
        return new EventSubscribeFlux<>(subscription, this, Function.identity());
    }

    @Override
    public <T> Flux<T> subscribe(Subscription subscription, Class<T> type) {
        return new EventSubscribeFlux<>(
            subscription,
            this,
            payload -> {
                try {
                    return payload.decode(type);
                } catch (Throwable e) {
                    log.error("decode message [{}] error", payload.getTopic(), e);
                }
                return null;
            });
    }

    @Override
    public Cancelable subscribe(Subscription subscription,
                                Function<TopicPayload, Mono<Void>> handler) {
        return new LocalSubscriber(this, subscription, handler);
    }

    protected void subscribeCluster(SubscriptionInfo info) {
        // fixme 企业版支持集群订阅
    }

    protected void unsubscribeCluster(SubscriptionInfo info) {
        // fixme 企业版支持集群订阅
    }

    @Override
    public <T> Mono<Long> publish(String topic, Publisher<T> event) {

        return doPublish(
            topic,
            event,
            (t, e, p, s, f) -> Flux
                .from(e)
                .flatMap(val -> publishFromLocal(t, val, p, s, f))
                .then(),
            sub -> true);
    }

    @Override
    public <T> Mono<Long> publish(String topic, T event, Scheduler scheduler) {
        return this
            .publish(topic, event)
            .subscribeOn(scheduler);
    }

    @Override
    public <T> Mono<Long> publish(String topic, T event) {
        return doPublish(topic,
                         event,
                         this::publishFromLocal,
                         sub -> true);
    }

    @Override
    public <T> Mono<Long> publish(CharSequence topic, T event) {
        return doPublish(topic,
                         event,
                         this::publishFromLocal,
                         sub -> true);
    }

    @Override
    public <T> Mono<Long> publish(CharSequence topic, Publisher<T> event) {

        return doPublish(
            topic,
            event,
            (t, e, p, s, f) -> Flux
                .from(e)
                .flatMap(val -> publishFromLocal(t, val, p, s, f))
                .then(),
            sub -> true);
    }


    @Override
    public <T> Mono<Long> publish(CharSequence topic, Supplier<T> event) {
        return doPublish(
            topic,
            event,
            (t,
             e,
             p,
             s,
             f) -> publishFromLocal(t, e.get(), p, s, f),
            sub -> sub.hasFeature(Subscription.Feature.local));
    }

    private Mono<Void> doPublish0(CharSequence topic,
                                  TopicPayload payload,
                                  Collection<SubscriptionInfo> subs,
                                  Map<CharSequence, String> published,
                                  ContextView ctx) {

        int subSize = subs.size();

        //只有一个订阅者时,不需要排序,减少内存占用
        TreeMap<Integer, Map<Object, Mono<Void>>> priority = subSize == 1 ? null : new TreeMap<>();

        Mono<Void> task = null;


        BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> handler;

        for (SubscriptionInfo sub : subs) {
            log.trace("publish {} to {}", topic, sub);
            handler = sub.handler;
            task = handler.apply(sub, payload);

            if (subSize > 1 && task != null) {
                priority
                    .computeIfAbsent(sub.priority, ignore -> Maps.newHashMapWithExpectedSize(subSize))
                    .put(handler, task);
            }
        }

        if (task == null) {
            return Mono.empty();
        }


        if (subSize != 1) {
            if (priority.size() == 1) {
                Collection<Mono<Void>> handlers = priority.firstEntry().getValue().values();
                if (handlers.size() == 1) {
                    task = handlers.iterator().next();
                } else {
                    task = Flux.merge(handlers).then();
                }
            } else {
                Set<Object> distinct = DISTINCT.get();
                Flux<Void> all = null;
                try {
                    //fixme 更高效的推送方式?
                    for (Map<Object, Mono<Void>> value : priority.values()) {
                        Flux<Void> that = null;
                        for (Map.Entry<Object, Mono<Void>> entry : value.entrySet()) {
                            //去重,防止同一个集群节点重复推送
                            if (!distinct.add(entry.getKey())) {
                                continue;
                            }
                            if (that == null) {
                                that = entry.getValue().flux();
                            } else {
                                //同一个优先级并行
                                that = that.mergeWith(entry.getValue());
                            }
                        }
                        if (that == null) {
                            continue;
                        }
                        if (all == null) {
                            all = that;
                        } else {
                            //不同优先级之间串行
                            all = all.concatWith(that);
                        }
                    }
                } finally {
                    distinct.clear();
                }
                task = all == null ? Mono.empty() : all.then();
            }
        }

        return task;
    }

    private <T> Mono<Void> publishFromLocal(CharSequence topic,
                                            T value,
                                            Collection<SubscriptionInfo> subs,
                                            Map<CharSequence, String> published,
                                            ContextView ctx) {
        TopicPayload payload = TopicPayload.of(topic, value);
        payload.addHeader(SHARE_PUBLISHED.getKey(), published);

        TraceHolder.writeContextTo(ctx, payload, TopicPayload::addHeader);

        return doPublish0(topic, payload, subs, published, ctx);
    }

    private <T> Mono<Long> doPublish(CharSequence topic,
                                     T arg,
                                     Function5<CharSequence,
                                         T,
                                         Collection<SubscriptionInfo>,
                                         Map<CharSequence, String>,
                                         ContextView,
                                         Mono<Void>> handler,
                                     EventPublisher.SubscriptionFilter predicate) {
        return new EventPublisher<>(this, topic, predicate, arg, handler);
    }

    @Service("eventbus")
    public interface S {

        @ServiceMethod("s")
        Mono<Void> sub(ByteBuf info);

        @ServiceMethod("u")
        Mono<Void> unsub(ByteBuf info);

        @ServiceMethod("p")
        Mono<Long> pub(ByteBuf payload);

    }

    @EqualsAndHashCode(
        of = {"id", "subscriber0", "topicRef"},
        cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SubscriptionInfo implements Disposable, Jsonable {
        //相对时间2024年
        private static final long baseTime = 1704038400000L;

        static final long RANDOM_MASK = (1L << 24) - 1;

        static final AtomicLongFieldUpdater<SubscriptionInfo> IN =
            AtomicLongFieldUpdater.newUpdater(SubscriptionInfo.class, "in"),
            OUT = AtomicLongFieldUpdater.newUpdater(SubscriptionInfo.class, "out"),
            ERROR = AtomicLongFieldUpdater.newUpdater(SubscriptionInfo.class, "error"),
            DROPPED = AtomicLongFieldUpdater.newUpdater(SubscriptionInfo.class, "dropped");

        static final Comparator<SubscriptionInfo> comparatorByTime = Comparator
            .comparingLong(SubscriptionInfo::getTime)
            .thenComparing(SubscriptionInfo::getId);

        static final Comparator<SubscriptionInfo> comparatorByLoad = Comparator
            .comparingLong(SubscriptionInfo::getIn)
            .thenComparingLong(SubscriptionInfo::getId);

        long id;

        public static long newId(long time) {
            if (time == 0) {
                time = System.currentTimeMillis();
            }
            //前40位为相对时间戳,后24位为随机
            long t = Math.abs(time - baseTime);
            int r = newRandom();
            return (t << 24) | (r & RANDOM_MASK);
        }

        public static int newRandom() {
            return ThreadLocalRandom
                .current()
                .nextInt(65535, 1 << 24);
        }

        public long readRandom() {
            return (id & RANDOM_MASK);
        }

        public static long readTime(long id) {
            return (id >> 24) + baseTime;
        }

        public long getTime() {
            return readTime(id);
        }

        @JsonIgnore
        private CharSequence subscriber0;
        @JsonIgnore
        private SharedPathString topic;

        private long features;

        @JsonIgnore
        public SeparatedCharSequence getTopic() {
            return topic == null ? topicRef : topic;
        }

        @JsonIgnore
        public String getTopicString() {
            return topic == null ? topicRef.getTopic() : topic.toString();
        }

        public boolean isCluster(){
            return false;
        }
        public String serverId() {
            // fixme 企业版支持集群
            return "default";
        }

        @JsonIgnore
        @Setter(AccessLevel.PRIVATE)
        transient BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> handler;
        @JsonIgnore
        transient Topic<SubscriptionInfo> topicRef;

        private int priority;

        private transient volatile long in, out, error, dropped = 0;


        public String getSubscriber() {
            return subscriber0.toString();
        }

        public static SubscriptionInfo of(Subscription subscription,
                                          Topic<SubscriptionInfo> topic,
                                          BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> handler) {
            SubscriptionInfo info = new SubscriptionInfo();
            info.id = newId(subscription.getTime());
            info.topicRef = topic;
            info.subscriber0 = SeparatedString.of(':', subscription.getSubscriber());
            info.features = EnumDict.toMask(subscription.getFeatures());
            info.priority = subscription.getPriority();
            info.handler = handler;
            return info;
        }

        public void dropped(TopicPayload payload) {
            DROPPED.incrementAndGet(this);
        }

        public void error(Throwable error) {
            ERROR.incrementAndGet(this);
        }

        public void in() {
            IN.incrementAndGet(this);
        }

        public void out() {
            OUT.incrementAndGet(this);
        }

        @Override
        public void dispose() {

        }

        SubscriptionInfo intern() {
            if (this.subscriber0 instanceof SeparatedCharSequence) {
                SeparatedCharSequence c = (SeparatedCharSequence) this.subscriber0;
                this.subscriber0 = c.internInner().intern();
            } else {
                this.subscriber0 = RecyclerUtils.intern(this.subscriber0);
            }
            return this;
        }

        public void setTopicRef(Topic<SubscriptionInfo> topicRef) {
            this.topicRef = topicRef;
            // 清空topic缓存,减少内存占用.
            this.topic = null;
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
        public String toString() {
            return getSubscriber() + "::" + getTopicString();
        }

        @Override
        public JSONObject toJson() {
            JSONObject obj = ObjectMappers.parseJson(
                ObjectMappers.toJsonBytes(this), JSONObject.class
            );
            obj.put("features", Lists.transform(
                EnumDict.getByMask(Subscription.Feature.class, features),
                Subscription.Feature::getValue));
            return obj;
        }
    }


    private ObjectName objectName;

    void registerMBean() {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            mBeanServer.registerMBean(new StandardMBean(new EventBusMBeanImpl(), EventBusMBean.class),
                                      objectName = new ObjectName("org.jetlinks:type=EventBus,name=InternalEventBus"));
        } catch (InstanceAlreadyExistsException ignore) {

        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    void unregisterMBean() {
        try {
            if (objectName != null) {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                mBeanServer.unregisterMBean(objectName);
            }
        } catch (Throwable ignore) {
        }
    }

    class EventBusMBeanImpl implements EventBusMBean {

        @Override
        public int getMaxBufferSize() {
            return maxBufferSize;
        }

        @Override
        public void setMaxBufferSize(int bufferSize) {
            maxBufferSize = bufferSize;
        }

        @Override
        public long getTotalSubscribers() {
            return root.getTotalSubscriber();
        }

        @Override
        public long getTotalTopics() {
            return root.getTotalTopic();
        }

        @Override
        public Set<SubscriptionInfo> getSubscribers(String topic) {
            return root
                .getTopic(topic)
                .map(Topic::getSubscribers)
                .orElse(Collections.emptySet());
        }


        @Override
        public TopicView view(String prefix) {
            return root
                .getTopic(prefix)
                .map(Topic::view)
                .orElse(null);
        }

        @Override
        public void cleanup() {
            this.cleanup();
        }
    }

    public interface EventBusMBean {

        int getMaxBufferSize();

        void setMaxBufferSize(int bufferSize);

        long getTotalSubscribers();

        long getTotalTopics();

        Set<SubscriptionInfo> getSubscribers(String topic);

        void cleanup();

        TopicView view(String prefix);
    }
}
