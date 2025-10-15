package org.jetlinks.supports.event;

import jakarta.annotation.Nonnull;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.jetlinks.supports.event.InternalEventBus.*;

class EventSubscribeFlux<T> extends Flux<T> implements org.reactivestreams.Subscription, Scannable,
    BiFunction<SubscriptionInfo, TopicPayload, Mono<Void>> {
    private Subscription subscription;
    private final Function<TopicPayload, T> converter;
    private final InternalEventBus parent;
    private final Consumer<TopicPayload> dropListener;

    volatile boolean cancelled;
//    volatile boolean waitingRecycle;

    volatile int wip;

    @SuppressWarnings("all")
    static final AtomicIntegerFieldUpdater<EventSubscribeFlux> WIP =
        AtomicIntegerFieldUpdater.newUpdater(EventSubscribeFlux.class,
                                             "wip");

    volatile long requested;
    @SuppressWarnings("all")
    static final AtomicLongFieldUpdater<EventSubscribeFlux> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(EventSubscribeFlux.class,
                                          "requested");
    volatile long remainder;
    @SuppressWarnings("all")
    static final AtomicLongFieldUpdater<EventSubscribeFlux> REMAINDER =
        AtomicLongFieldUpdater.newUpdater(EventSubscribeFlux.class,
                                          "remainder");
    private Attr.RunStyle runStyle;
    private LocalSubscriber subscriber;
    private CoreSubscriber<? super T> actual;
    private volatile Queue<TopicPayload> buffer;

    EventSubscribeFlux(Subscription subscription,
                       InternalEventBus parent,
                       Function<TopicPayload, T> converter) {
        this.subscription = subscription;
        this.dropListener = subscription.getDropListener();
        this.parent = parent;
        this.converter = converter;
        if (parent.maxBufferSize < 0) {
            remainder = Long.MAX_VALUE;
        }
    }

    @Override
    public void subscribe(@Nonnull CoreSubscriber<? super T> actual) {
        synchronized (this) {
            if (subscriber != null) {
                actual.onError(Exceptions.duplicateOnSubscribeException());
                return;
            }
            this.actual = actual;
            this.runStyle = Scannable.from(actual).scan(Attr.RUN_STYLE);
            this.subscriber = new LocalSubscriber(parent, subscription, this);
        }
        //release memory
        this.subscription = null;
        actual.onSubscribe(this);

    }

    @Override
    public Mono<Void> apply(SubscriptionInfo info, TopicPayload payload) {
        next(info, payload);
        return Mono.empty();
    }

    void dropped(TopicPayload payload) {
        if (dropListener == null) {
            return;
        }
        dropListener.accept(payload);
    }

    private void tryCreateBuffer() {
        if (requested < Integer.MAX_VALUE || this.runStyle != Attr.RunStyle.ASYNC) {
            if (buffer == null) {
                synchronized (this) {
                    if (buffer == null) {
                        buffer = newBuffer();
                    }
                }
            }
        }
    }

    void next(SubscriptionInfo info, TopicPayload payload) {
        tryCreateBuffer();

        Queue<TopicPayload> buffer = this.buffer;
        if (buffer != null) {
            //不丢弃数据
            if (remainder == Long.MAX_VALUE) {
                if (!buffer.offer(payload)) {
                    info.dropped(payload);
                    dropped(payload);
                }
            } else {
                long size = REMAINDER.incrementAndGet(this);
                if (size >= parent.maxBufferSize) {
                    info.dropped(payload);
                    dropped(payload);
                    REMAINDER.decrementAndGet(this);
                } else {
                    if (!buffer.offer(payload)) {
                        info.dropped(payload);
                        dropped(payload);
                        REMAINDER.decrementAndGet(this);
                    }
                }
            }
            drain();
        } else {
            next0(payload);
        }
    }

    private void next0(TopicPayload payload) {
        T result = converter.apply(payload);
        if (result != null) {
            actual.onNext(result);
        } else {
            Operators.onDiscard(payload, actual.currentContext());
            dropped(payload);
        }
    }

    @SuppressWarnings("all")
    void drain() {
        //还没有数据产生或者请求了Long.MAX_VALUE
        Queue<TopicPayload> buffer = this.buffer;
        if (buffer == null) {
            return;
        }
        if (WIP.getAndIncrement(this) != 0) {
            return;
        }
        Subscriber<? super T> a = actual;

        int missed = 1;

        for (; ; ) {

            long r = requested;
            long e = 0L;

            while (r != e) {
                boolean d = cancelled;

                TopicPayload t = buffer.poll();
                boolean empty = t == null;

                if (checkTerminated(d, a)) {
                    return;
                }

                if (empty) {
                    break;
                }

                next0(t);

                e++;
            }

            if (r == e) {
                if (checkTerminated(cancelled, a)) {
                    return;
                }
            }

            if (e != 0L) {
                if (remainder != Long.MAX_VALUE) {
                    Operators.produced(REMAINDER, this, e);
                }
                if (r != Long.MAX_VALUE) {
                    Operators.produced(REQUESTED, this, e);
                }
            }

            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                break;
            }
        }
    }


    boolean checkTerminated(boolean done, Subscriber<? super T> a) {
        if (done) {
            a.onComplete();
            return true;
        }
        return false;
    }

    private Queue<TopicPayload> newBuffer() {
        return Queues.<TopicPayload>unboundedMultiproducer().get();
    }

    @Override
    public void request(long n) {
        if (Operators.validate(n)) {
            Operators.addCap(REQUESTED, this, n);
            drain();
        }
    }

    @Override
    public void cancel() {
        synchronized (this) {
            if (cancelled) {
                return;
            }
            cancelled = true;
        }
        Queue<TopicPayload> buffer = this.buffer;
        if (buffer != null) {
            for (TopicPayload payload;
                 (payload = buffer.poll()) != null; ) {
                Operators.onDiscard(payload, actual.currentContext());
                dropped(payload);
            }
        }
        if (null != subscriber) {
            subscriber.dispose();
        }

    }

    @Override
    public Object scanUnsafe(@Nonnull Attr key) {
        if (key == Attr.ACTUAL) return actual;
        if (key == Attr.BUFFERED) return buffer == null ? 0 : buffer.size();
        if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
        if (key == Attr.CANCELLED) return cancelled;
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

        return null;
    }

}