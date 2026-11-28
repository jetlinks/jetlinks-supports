package org.jetlinks.supports.rpc;

import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.*;

/** One subscription owns context capture, request resources, response delivery and its deadline. */
final class RpcCallMono<R extends Disposable, T> extends Mono<T> implements Scannable {
    /** Immutable method plan; request state is acquired afresh for every subscription. */
    abstract static class Plan<R extends Disposable, T> {
        final Duration timeout;
        final Scheduler scheduler;
        final long timeoutNanos;
        Plan(Duration timeout, Scheduler scheduler) {
            this.timeout = timeout; this.scheduler = scheduler;
            timeoutNanos = timeout == null ? 0 : timeout.toNanos();
        }
        abstract R acquire(Context context, Object request);
        abstract Mono<?> invoke(R resource, Object request);
        /** Consumes the input reference; the returned value transfers separately to the downstream. */
        abstract T decode(Object value);
    }

    final Plan<R, T> plan;
    final Object request;
    RpcCallMono(Plan<R, T> plan, Object request) { this.plan = plan; this.request = request; }
    static <R extends Disposable, T> Mono<T> create(Plan<R, T> plan, Object request) {
        return onAssembly(new RpcCallMono<>(plan, request));
    }

    @Override public void subscribe(CoreSubscriber<? super T> actual) {
        CallSubscriber<R, T> subscriber = new CallSubscriber<>(actual, plan);
        actual.onSubscribe(subscriber);
        try {
            if (subscriber.state != 0) return;
            R resource = plan.acquire(actual.currentContext(), request);
            subscriber.resource = resource;
            if (subscriber.state != 0) return;
            Mono<?> source = plan.invoke(resource, request);
            if (subscriber.state == 0) source.subscribe(subscriber);
        } catch (Throwable error) {
            subscriber.onError(Operators.onOperatorError(error, actual.currentContext()));
        } finally { subscriber.initialized(); }
    }

    @Override public Object scanUnsafe(Attr key) { return key == Attr.RUN_STYLE ? Attr.RunStyle.ASYNC : null; }

    static final class CallSubscriber<R extends Disposable, T> extends Operators.DeferredSubscription implements CoreSubscriber<Object>, Runnable {
        private static final AtomicIntegerFieldUpdater<CallSubscriber> STATE = AtomicIntegerFieldUpdater.newUpdater(CallSubscriber.class, "state");
        private static final AtomicReferenceFieldUpdater<CallSubscriber, Disposable> TIMER = AtomicReferenceFieldUpdater.newUpdater(CallSubscriber.class, Disposable.class, "timer");
        private static final AtomicReferenceFieldUpdater<CallSubscriber, Disposable> RESOURCE = AtomicReferenceFieldUpdater.newUpdater(CallSubscriber.class, Disposable.class, "resource");
        private static final Disposable DISPOSED = Disposables.disposed();
        private final CoreSubscriber<? super T> actual;
        private final Plan<R, T> plan;
        private volatile int state; // 0 active, 1 terminal, 2 cancelled
        private volatile Disposable timer, resource;
        private volatile boolean initializing = true;

        CallSubscriber(CoreSubscriber<? super T> actual, Plan<R, T> plan) { this.actual = actual; this.plan = plan; }
        @Override public Context currentContext() { return actual.currentContext(); }

        @Override public void onSubscribe(Subscription subscription) {
            if (state != 0) { subscription.cancel(); return; }
            if (plan.timeout != null) {
                try {
                    Disposable task = plan.scheduler.schedule(this, plan.timeoutNanos, TimeUnit.NANOSECONDS);
                    if (!TIMER.compareAndSet(this, null, task)) task.dispose();
                } catch (Throwable error) {
                    subscription.cancel(); Exceptions.throwIfFatal(error); onError(error); return;
                }
            }
            set(subscription);
        }

        @Override public void onNext(Object input) {
            if (state != 0) { ReferenceCountUtil.release(input); return; }
            T value;
            try { value = plan.decode(input); }
            catch (Throwable error) { onError(Operators.onOperatorError(error, currentContext())); return; }
            if (!STATE.compareAndSet(this, 0, 1)) { ReferenceCountUtil.release(value); return; }
            cancelTimer();
            try { if (value != null) actual.onNext(value); actual.onComplete(); }
            finally { cleanup(); }
        }

        @Override public void onError(Throwable error) {
            if (STATE.compareAndSet(this, 0, 1)) {
                cancelTimer();
                try { actual.onError(error); } finally { cleanup(); }
            } else Operators.onErrorDropped(error, currentContext());
        }
        @Override public void onComplete() {
            if (STATE.compareAndSet(this, 0, 1)) {
                cancelTimer();
                try { actual.onComplete(); } finally { cleanup(); }
            }
        }
        @Override public void run() {
            if (STATE.compareAndSet(this, 0, 1)) {
                cancelTimer(); super.cancel();
                try { actual.onError(new TimeoutException("RPC request timeout after " + plan.timeout)); }
                finally { cleanup(); }
            }
        }
        @Override public void cancel() {
            if (STATE.compareAndSet(this, 0, 2)) {
                cancelTimer();
                try { super.cancel(); } finally { cleanup(); }
            }
        }
        private void cancelTimer() {
            if (plan.timeout == null) return;
            Disposable task = TIMER.getAndSet(this, DISPOSED);
            if (task != null && task != DISPOSED) task.dispose();
        }
        private void initialized() {
            initializing = false;
            if (state != 0) cleanup();
        }
        private void cleanup() {
            // Cancel may race capture or source construction; their borrowed request stays live until subscribe returns.
            if (initializing) return;
            Disposable scope = RESOURCE.getAndSet(this, null);
            if (scope != null) {
                try { scope.dispose(); }
                catch (Throwable error) { Exceptions.throwIfFatal(error); Operators.onErrorDropped(error, currentContext()); }
            }
        }
    }
}
