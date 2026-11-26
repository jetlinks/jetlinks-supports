package org.jetlinks.supports.rpc;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.*;
import reactor.util.context.Context;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.Assert.*;

public class RpcCallMonoTest {
    static final class Scope implements Disposable {
        final String value;
        final AtomicInteger disposed = new AtomicInteger();
        Scope(String value) { this.value = value; }
        public void dispose() { assertEquals(1, disposed.incrementAndGet()); }
        public boolean isDisposed() { return disposed.get() != 0; }
    }
    abstract static class Plan extends RpcCallMono.Plan<Scope, String> {
        Plan() { super(null, null); }
        String decode(Object value) { return (String) value; }
    }

    @Test public void cancellationDuringCaptureWaitsForInitializationAndNeverStartsTheSource() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Scope scope = new Scope("capture"); AtomicInteger invoked = new AtomicInteger();
        BaseSubscriber<String> actual = new BaseSubscriber<>() {
            protected void hookOnSubscribe(Subscription subscription) { request(1); }
            protected void hookOnNext(String value) { fail("cancelled subscriber received a value"); }
        };
        var call = RpcCallMono.create(new Plan() {
            Scope acquire(Context context, Object request) {
                entered.countDown();
                try { assertTrue(resume.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new AssertionError(error); }
                assertFalse(scope.isDisposed()); return scope;
            }
            Mono<?> invoke(Scope resource, Object request) { invoked.incrementAndGet(); return Mono.just("wrong"); }
        }, null);
        try {
            Future<?> subscription = executor.submit(() -> call.subscribe(actual));
            assertTrue(entered.await(5, TimeUnit.SECONDS)); actual.dispose(); assertFalse(scope.isDisposed());
            resume.countDown(); subscription.get(5, TimeUnit.SECONDS);
            assertEquals(1, scope.disposed.get()); assertEquals(0, invoked.get());
        } finally { resume.countDown(); executor.shutdownNow(); }
    }

    @Test public void cancellationDuringSourceConstructionDoesNotReleaseItsBorrowEarly() {
        Scope scope = new Scope("source"); AtomicInteger subscribed = new AtomicInteger();
        BaseSubscriber<String> actual = new BaseSubscriber<>() {};
        RpcCallMono.create(new Plan() {
            Scope acquire(Context context, Object request) { return scope; }
            Mono<?> invoke(Scope resource, Object request) {
                actual.dispose(); assertFalse(resource.isDisposed());
                return Mono.defer(() -> { subscribed.incrementAndGet(); return Mono.just("wrong"); });
            }
        }, null).subscribe(actual);
        assertEquals(1, scope.disposed.get()); assertEquals(0, subscribed.get());
    }

    @Test public void reusedCallReadsContextAndAcquiresFreshResourcesForEachSubscription() {
        var scopes = new java.util.ArrayList<Scope>();
        Mono<String> call = RpcCallMono.create(new Plan() {
            Scope acquire(Context context, Object request) { Scope scope = new Scope(context.get("route")); scopes.add(scope); return scope; }
            Mono<?> invoke(Scope resource, Object request) { return Mono.just(resource.value); }
        }, null);
        reactor.test.StepVerifier.create(call.contextWrite(Context.of("route", "a"))).expectNext("a").verifyComplete();
        reactor.test.StepVerifier.create(call.contextWrite(Context.of("route", "b"))).expectNext("b").verifyComplete();
        assertEquals(2, scopes.size()); scopes.forEach(scope -> assertEquals(1, scope.disposed.get()));
    }

    @Test public void sourceFactoryFailureAndEmptyCompletionReleaseTheScopeOnce() {
        for (boolean fail : new boolean[]{false, true}) {
            Scope scope = new Scope("terminal"); Exception error = new java.io.IOException("source");
            Mono<String> call = RpcCallMono.create(new Plan() {
                Scope acquire(Context context, Object request) { return scope; }
                Mono<?> invoke(Scope resource, Object request) { if (fail) throw Exceptions.propagate(error); return Mono.empty(); }
            }, null);
            if (fail) reactor.test.StepVerifier.create(call).expectErrorMatches(value -> value == error).verify();
            else reactor.test.StepVerifier.create(call).verifyComplete();
            assertEquals(1, scope.disposed.get());
        }
    }

    @Test public void explicitlyConfiguredTimeoutCancelsTheSourceAndReleasesTheScope() {
        var clock = reactor.test.scheduler.VirtualTimeScheduler.create();
        Scope scope = new Scope("timeout");
        AtomicInteger cancelled = new AtomicInteger();
        try {
            Mono<String> call = RpcCallMono.create(new RpcCallMono.Plan<Scope, String>(java.time.Duration.ofSeconds(3), clock) {
                Scope acquire(Context context, Object request) { return scope; }
                Mono<?> invoke(Scope resource, Object request) { return Mono.never().doOnCancel(cancelled::incrementAndGet); }
                String decode(Object value) { return (String) value; }
            }, null);
            reactor.test.StepVerifier.create(call)
                .then(() -> clock.advanceTimeBy(java.time.Duration.ofSeconds(3)))
                .expectError(TimeoutException.class).verify(java.time.Duration.ofSeconds(1));
            assertEquals(1, cancelled.get());
            assertEquals(1, scope.disposed.get());
        } finally { clock.dispose(); }
    }
}
