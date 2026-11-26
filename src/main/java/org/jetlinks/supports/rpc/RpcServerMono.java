package org.jetlinks.supports.rpc;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;
import java.util.concurrent.atomic.*;

/** Inbound unary execution with one owned request, one pending response and demand-aware terminal delivery. */
final class RpcServerMono extends Mono<Payload> implements Scannable {
    abstract static class Plan {
        abstract AtomicInteger acquire();
        abstract Mono<?> invoke(Payload request, CallSubscriber subscriber);
        abstract Payload encode(Object value);
    }
    private final Plan plan;
    private final Payload request;
    private RpcServerMono(Plan plan, Payload request) { this.plan = plan; this.request = request; }
    static Mono<Payload> create(Plan plan, Payload request) { return onAssembly(new RpcServerMono(plan, request)); }

    @Override public void subscribe(CoreSubscriber<? super Payload> actual) {
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
        private static final AtomicReferenceFieldUpdater<CallSubscriber, Payload> OUTPUT = AtomicReferenceFieldUpdater.newUpdater(CallSubscriber.class, Payload.class, "output");
        private static final AtomicReferenceFieldUpdater<CallSubscriber, Payload> REQUEST = AtomicReferenceFieldUpdater.newUpdater(CallSubscriber.class, Payload.class, "request");
        private final CoreSubscriber<? super Payload> actual;
        private final Plan plan;
        private volatile int state;
        private volatile Payload request, output;
        private AtomicInteger slot;
        private volatile boolean initializing = true;
        Context context;

        CallSubscriber(CoreSubscriber<? super Payload> actual, Plan plan) {
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
            Payload response;
            try { response = plan.encode(value); }
            catch (Throwable error) { failure(Operators.onOperatorError(error, currentContext())); return; }
            publish(response);
        }
        @Override public void onComplete() {
            if (claim()) publish(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER));
        }
        @Override public void onError(Throwable error) {
            if (claim()) failure(error); else Operators.onErrorDropped(error, currentContext());
        }
        private void failure(Throwable error) {
            Payload response;
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
        private void publish(Payload response) {
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
                Payload response = OUTPUT.getAndSet(this, null);
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
        private void discard() { Payload value = OUTPUT.getAndSet(this, null); if (value != null) value.release(); }
        private void cleanup() {
            // Initial parsing/context restore may still borrow the request after downstream cancellation.
            if (initializing || (state & (CANCELLED | EMITTED)) == 0) return;
            Payload value = REQUEST.getAndSet(this, null);
            if (value != null) try { value.release(); } finally { if (slot != null) slot.decrementAndGet(); }
        }
    }
}
