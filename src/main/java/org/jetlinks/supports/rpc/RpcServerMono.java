package org.jetlinks.supports.rpc;

import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.jetlinks.core.rpc.transport.RpcFrame;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;
import java.util.concurrent.atomic.*;

/** Inbound unary execution with one owned request, one pending response and demand-aware terminal delivery. */
final class RpcServerMono extends Mono<RpcFrame> implements Scannable {
    abstract static class Plan {
        abstract AtomicInteger acquire();
        abstract Mono<?> invoke(RpcFrame request, CallSubscriber subscriber);
        abstract RpcFrame encode(Object value);
    }
    private final Plan plan;
    private final RpcFrame request;
    private RpcServerMono(Plan plan, RpcFrame request) { this.plan = plan; this.request = request; }
    static Mono<RpcFrame> create(Plan plan, RpcFrame request) { return onAssembly(new RpcServerMono(plan, request)); }

    @Override public void subscribe(CoreSubscriber<? super RpcFrame> actual) {
        CallSubscriber subscriber = new CallSubscriber(actual, plan);
        try { request.retain(); }
        catch (Throwable error) { Operators.error(actual, Operators.onOperatorError(error, actual.currentContext())); return; }
        subscriber.request = request;
        try {
            actual.onSubscribe(subscriber);
            if (subscriber.cancelled()) return;
            subscriber.slot = plan.acquire();
            if (subscriber.cancelled()) return;
            Mono<?> source = plan.invoke(request, subscriber);
            if (!subscriber.cancelled()) source.subscribe(subscriber);
        } catch (Throwable error) {
            subscriber.onError(Operators.onOperatorError(error, subscriber.currentContext()));
        } finally {
            subscriber.initializing = false;
            subscriber.cleanup();
        }
    }
    @Override public Object scanUnsafe(Attr key) { return key == Attr.RUN_STYLE ? Attr.RunStyle.ASYNC : null; }

    static final class CallSubscriber extends Operators.DeferredSubscription implements CoreSubscriber<Object> {
        private static final int REQUESTED = 1, READY = 2, CANCELLED = 4, EMITTED = 8, CLAIMED = 16;
        private static final AtomicIntegerFieldUpdater<CallSubscriber> STATE = AtomicIntegerFieldUpdater.newUpdater(CallSubscriber.class, "state");
        private static final AtomicReferenceFieldUpdater<CallSubscriber, RpcFrame> OUTPUT = AtomicReferenceFieldUpdater.newUpdater(CallSubscriber.class, RpcFrame.class, "output");
        private static final AtomicReferenceFieldUpdater<CallSubscriber, RpcFrame> REQUEST = AtomicReferenceFieldUpdater.newUpdater(CallSubscriber.class, RpcFrame.class, "request");
        private final CoreSubscriber<? super RpcFrame> actual;
        private final Plan plan;
        private volatile int state;
        private volatile RpcFrame request, output;
        private AtomicInteger slot;
        private volatile boolean initializing = true;
        Context context;

        CallSubscriber(CoreSubscriber<? super RpcFrame> actual, Plan plan) {
            this.actual = actual; this.plan = plan; context = actual.currentContext();
        }
        @Override public Context currentContext() { return context; }
        boolean cancelled() { return (state & CANCELLED) != 0; }
        @Override public void onSubscribe(Subscription subscription) { set(subscription); }
        @Override public void request(long n) {
            if (!Operators.validate(n)) return;
            for (;;) {
                int before = state;
                if ((before & (REQUESTED | CANCELLED | EMITTED)) != 0) return;
                if (STATE.compareAndSet(this, before, before | REQUESTED)) {
                    if ((before & READY) != 0) drain(); else super.request(1);
                    return;
                }
            }
        }
        private boolean claim() {
            for (;;) {
                int before = state;
                if ((before & (CLAIMED | CANCELLED)) != 0) return false;
                if (STATE.compareAndSet(this, before, before | CLAIMED)) return true;
            }
        }
        @Override public void onNext(Object value) {
            if (!claim()) { ReferenceCountUtil.release(value); return; }
            RpcFrame response;
            try { response = plan.encode(value); }
            catch (Throwable error) { failure(Operators.onOperatorError(error, currentContext())); return; }
            publish(response);
        }
        @Override public void onComplete() {
            if (claim()) publish(new RpcFrame(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER));
        }
        @Override public void onError(Throwable error) {
            if (claim()) failure(error); else Operators.onErrorDropped(error, currentContext());
        }
        private void failure(Throwable error) {
            RpcFrame response;
            try { response = RpcProtocol.error(error); }
            catch (Throwable encodingError) {
                Exceptions.throwIfFatal(encodingError);
                for (;;) {
                    int before = state;
                    if ((before & CANCELLED) != 0) { Operators.onErrorDropped(encodingError, currentContext()); return; }
                    if (STATE.compareAndSet(this, before, before | EMITTED)) break;
                }
                try { actual.onError(encodingError); } finally { cleanup(); }
                return;
            }
            publish(response);
        }
        private void publish(RpcFrame response) {
            output = response;
            for (;;) {
                int before = state;
                if ((before & CANCELLED) != 0) { discard(); return; }
                if (STATE.compareAndSet(this, before, before | READY)) break;
            }
            drain();
        }
        private void drain() {
            for (;;) {
                int before = state;
                if ((before & (REQUESTED | READY)) != (REQUESTED | READY) || (before & (CANCELLED | EMITTED)) != 0) return;
                if (!STATE.compareAndSet(this, before, before | EMITTED)) continue;
                RpcFrame response = OUTPUT.getAndSet(this, null);
                try { actual.onNext(response); actual.onComplete(); }
                finally { cleanup(); }
                return;
            }
        }
        @Override public void cancel() {
            for (;;) {
                int before = state;
                if ((before & (CANCELLED | EMITTED)) != 0) return;
                if (STATE.compareAndSet(this, before, before | CANCELLED)) break;
            }
            try { super.cancel(); } finally { discard(); cleanup(); }
        }
        private void discard() { RpcFrame value = OUTPUT.getAndSet(this, null); if (value != null) value.release(); }
        private void cleanup() {
            // Initial parsing/context restore may still borrow the request after downstream cancellation.
            if (initializing || (state & (CANCELLED | EMITTED)) == 0) return;
            RpcFrame value = REQUEST.getAndSet(this, null);
            if (value != null) try { value.release(); } finally { if (slot != null) slot.decrementAndGet(); }
        }
    }
}
