package org.jetlinks.supports.event;

import com.google.common.collect.Sets;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.ThreadLocalRandom;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.jetlinks.core.Routable;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.topic.Topic;
import org.jetlinks.core.utils.HashUtils;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.function.Function5;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import static org.jetlinks.supports.event.InternalEventBus.SubscriptionInfo;

@RequiredArgsConstructor
class EventPublisher<T> extends Mono<Long> implements Consumer<Topic<SubscriptionInfo>>, Runnable {

    private static final FastThreadLocal<Set<SubscriptionInfo>> PUB_HANDLERS = new FastThreadLocal<>() {
        @Override
        protected Set<SubscriptionInfo> initialValue() {
            return new HashSet<>();
        }
    };

    private static final FastThreadLocal<Map<CharSequence, ShareSelector>> SHARED =
        new FastThreadLocal<Map<CharSequence, ShareSelector>>() {
            @Override
            protected Map<CharSequence, ShareSelector> initialValue() {
                return new HashMap<>();
            }
        };

    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<EventPublisher, Map>
        SHARED_UPDATER = AtomicReferenceFieldUpdater.newUpdater(EventPublisher.class, Map.class, "shared");

    private volatile Map<CharSequence, ShareSelector> shared;

    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<EventPublisher, Set>
        READY_TO_PUB_UPDATER = AtomicReferenceFieldUpdater.newUpdater(EventPublisher.class, Set.class, "readyToPub");

    private volatile Set<SubscriptionInfo> readyToPub;

    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<EventPublisher, Map>
        SHARE_PUBLISHED_UPDATER = AtomicReferenceFieldUpdater.newUpdater(EventPublisher.class, Map.class, "sharePublished");

    private volatile Map<CharSequence, String> sharePublished;

    private final InternalEventBus parent;
    private final CharSequence topic;
    private final SubscriptionFilter predicate;
    private final T target;
    private final Function5<CharSequence, T, Collection<InternalEventBus.SubscriptionInfo>, Map<CharSequence, String>, ContextView, Mono<Void>> handler;


    private Comparator<InternalEventBus.SubscriptionInfo> comparator;

    private Map<CharSequence, String> sharePublished() {
        return sharePublished == null
            ? sharePublished = new ConcurrentHashMap<>(shared.size())
            : sharePublished;
    }

    private Map<CharSequence, String> getAndReleaseSharePublished() {
        @SuppressWarnings("all")
        Map<CharSequence, String> sharePublished =
            SHARE_PUBLISHED_UPDATER.getAndSet(this, null);
        return sharePublished == null ? Collections.emptyMap() : sharePublished;
    }

    private Set<InternalEventBus.SubscriptionInfo> release() {
        @SuppressWarnings("all")
        Set<InternalEventBus.SubscriptionInfo> readyToPub = READY_TO_PUB_UPDATER.getAndSet(this, null);
        @SuppressWarnings("all")
        Map<CharSequence, ShareSelector> shared = SHARED_UPDATER.getAndSet(this, null);
        try {
            if (CollectionUtils.isEmpty(readyToPub)) {
                return Collections.emptySet();
            }
            return new HashSet<>(Sets.filter(readyToPub, predicate::ready));
        } finally {
            readyToPub.clear();
            shared.clear();

        }
    }


    @Override
    public void run() {
        Map<CharSequence, ShareSelector> shared = this.shared;
        if (MapUtils.isEmpty(shared)) {
            return;
        }
        Set<SubscriptionInfo> readyToPub = this.readyToPub;
        Map<CharSequence, String> sharePublished = this.sharePublished();
        //处理共享订阅
        for (ShareSelector value : shared.values()) {
            SubscriptionInfo first = value.current();
            sharePublished.put(first.getSubscriber0(), first.serverId());
            readyToPub.add(first);
        }
    }

    @Nonnull
    private Comparator<SubscriptionInfo> createHashComparator() {
        if (this.comparator == null) {
            if (target instanceof Routable routable) {
                return comparator = Comparator
                    .<SubscriptionInfo>comparingLong(
                        e ->routable.hash(e.serverId()))
                    .thenComparingLong(routable::hash);
            } else {
                return comparator = Comparator
                    .<SubscriptionInfo>comparingLong(
                        e -> HashUtils.murmur3_128(target, e.serverId()))
                    .thenComparingLong(e -> HashUtils.murmur3_128(target, e));
            }
        }
        return this.comparator;
    }

    @Override
    public void accept(Topic<SubscriptionInfo> subs) {
        Set<SubscriptionInfo> subscriptions = subs.getSubscribers();
        if (subscriptions.isEmpty()) {
            return;
        }
        for (SubscriptionInfo sub : subscriptions) {
            if (!predicate.prepare(sub)) {
                continue;
            }
            //共享订阅时,添加到缓存,最后再处理
            if (sub.hasFeature(Subscription.Feature.shared)) {
                getShareContainer(sub).add(sub);
                continue;
            }
            readyToPub.add(sub);
        }
    }

    @Override
    public void subscribe(@Nonnull CoreSubscriber<? super Long> actual) {

        if (!SHARED_UPDATER.compareAndSet(this, null, SHARED.get()) ||
            !READY_TO_PUB_UPDATER.compareAndSet(this, null, PUB_HANDLERS.get())) {
            Operators.error(actual,Exceptions.duplicateOnSubscribeException());
            return;
        }
        parent.root.findTopic(topic, this, this);

        Map<CharSequence, String> sharePublished = this.getAndReleaseSharePublished();
        Set<SubscriptionInfo> readyToPub = this.release();

        long size = readyToPub.size();

        //没有订阅者
        if (size == 0) {
            actual.onSubscribe(Operators.emptySubscription());
            actual.onNext(size);
            actual.onComplete();
            return;
        }

        Context context = actual.currentContext();
        handler
            .apply(topic, target, readyToPub, sharePublished, context)
            .then(Mono.just(size))
            .subscribe(actual);
    }


    private ShareSelector getShareContainer(SubscriptionInfo conn) {

        ShareSelector container = shared.get(conn.getSubscriber0());
        if (container != null) {
            return container;
        }
        //最先订阅的收到数据
        if (conn.hasFeature(Subscription.Feature.sharedOldest)) {
            container = conn.hasFeature(Subscription.Feature.sharedLocalFirst)
                ? new ShareLocalFirstSelector(SubscriptionInfo.comparatorByTime)
                : new ShareSelector(SubscriptionInfo.comparatorByTime);
        }
        //hashed方式路由
        else if (conn.hasFeature(Subscription.Feature.sharedHashed)) {
            container = conn.hasFeature(Subscription.Feature.sharedLocalFirst)
                ? new ShareLocalFirstSelector(createHashComparator())
                : new ShareSelector(createHashComparator());
        }
        //最小负载路由
        else if (conn.hasFeature(Subscription.Feature.sharedMinimumLoad)) {
            container = conn.hasFeature(Subscription.Feature.sharedLocalFirst)
                ? new ShareLocalFirstSelector(SubscriptionInfo.comparatorByLoad)
                : new ShareSelector(SubscriptionInfo.comparatorByLoad);
        }
        //默认
        else {
            container = conn.hasFeature(Subscription.Feature.sharedLocalFirst)
                ? new RandomLocalFirstShareSelector()
                : new RandomShareSelector();
        }

        if (shared.putIfAbsent(conn.getSubscriber0(), container) == null) {
            return container;
        }

        return shared.get(conn.getSubscriber0());
    }

    static class RandomLocalFirstShareSelector extends RandomShareSelector {
        private boolean hasLocal;

        @Override
        public void add(SubscriptionInfo sub) {
            if (!sub.isCluster()) {
                if (current == null || current.isCluster()) {
                    current = sub;
                } else {
                    super.add(sub);
                }
                hasLocal = true;
                return;
            }
            if (!hasLocal) {
                super.add(sub);
            }
        }
    }

    static class RandomShareSelector extends ShareSelector {
        final long seeds;
        long weight;

        RandomShareSelector() {
            super(null);
            this.seeds = ThreadLocalRandom.current().nextInt();
        }

        @Override
        public void add(SubscriptionInfo sub) {
            long weight = seeds ^ sub.readRandom();

            if (this.current == null) {
                this.current = sub;
                this.weight = weight;
            } else {
                if (weight < this.weight) {
                    this.current = sub;
                    this.weight = weight;
                }
            }
        }
    }

    static class ShareLocalFirstSelector extends ShareSelector {
        boolean hasLocal;

        ShareLocalFirstSelector(Comparator<SubscriptionInfo> comparator) {
            super(comparator);
        }

        @Override
        public void add(SubscriptionInfo sub) {
            if (!sub.isCluster()) {
                if (current == null || current.isCluster()) {
                    current = sub;
                } else {
                    super.add(sub);
                }
                hasLocal = true;
                return;
            }
            if (!hasLocal) {
                super.add(sub);
            }
        }
    }

    static class ShareSelector {
        private final Comparator<SubscriptionInfo> comparator;
        SubscriptionInfo current;

        ShareSelector(Comparator<SubscriptionInfo> comparator) {
            this.comparator = comparator;
        }

        public SubscriptionInfo current() {
            return current;
        }

        public void add(SubscriptionInfo subscriptionInfo) {
            if (current == null) {
                current = subscriptionInfo;
            } else {
                int c = comparator.compare(current, subscriptionInfo);
                if (c > 0) {
                    current = subscriptionInfo;
                }
            }
        }
    }

    interface SubscriptionFilter {

        boolean prepare(SubscriptionInfo info);

        default boolean ready(SubscriptionInfo info) {
            return true;
        }
    }
}
