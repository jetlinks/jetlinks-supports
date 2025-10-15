package org.jetlinks.supports.event;

import org.jetlinks.core.event.Subscription;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class InternalEventBusTest {


    @Test
    public void test() {
        InternalEventBus eventBus = new InternalEventBus();

        AtomicInteger count = new AtomicInteger();

        eventBus
            .subscribe(Subscription.of("test", "/test", Subscription.Feature.local))
            .doOnNext(v -> count.getAndIncrement())
            .subscribe();

        eventBus.publish("/test", 1)
                .as(StepVerifier::create)
                .expectNext(1L)
                .verifyComplete();

        assertEquals(1, count.get());

    }

    @Test
    public void testPa() {
        InternalEventBus eventBus = new InternalEventBus();

        AtomicInteger count = new AtomicInteger();

        eventBus
            .subscribe(Subscription.of("test", "/test", Subscription.Feature.local))
            .handle((v, s) -> {
                count.incrementAndGet();
                s.next(v);
            })
            .subscribe();

        Flux.range(0, 1000)
            .flatMap(i -> eventBus
                .publish("/test", 123)
                .subscribeOn(Schedulers.boundedElastic()))
            .then()
            .as(StepVerifier::create)
            .expectComplete()
            .verify();

        assertEquals(1000,count.get());

    }
}