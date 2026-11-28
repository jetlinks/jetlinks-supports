package org.jetlinks.supports.event;

import org.jetlinks.core.event.Subscription;
import org.junit.Test;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class LocalSubscriberObservationTest {
    @Test
    public void callbackReportsNoQueueWhilePublisherWaitsAndPreservesContextAndDisposal() {
        var bus = new InternalEventBus();
        var gate = Sinks.<Void>empty();
        var received = new AtomicInteger();
        var trace = new AtomicReference<String>();
        var handle = bus.subscribe(Subscription.of("callback", "/buffer", Subscription.Feature.local),
            payload -> reactor.core.publisher.Mono.deferContextual(ctx -> {
                received.incrementAndGet();
                trace.set(ctx.get("trace"));
                return gate.asMono();
            }));
        var scan = Scannable.from(handle);
        try {
            assertTrue(scan.isScanAvailable());
            assertEquals(0, scan.scanUnsafe(Scannable.Attr.BUFFERED));
            StepVerifier.create(bus.publish("/buffer", "value").contextWrite(Context.of("trace", "original")))
                .then(() -> {
                    assertEquals(1, received.get());
                    assertEquals("original", trace.get());
                    assertEquals(0, scan.scanUnsafe(Scannable.Attr.BUFFERED));
                    gate.tryEmitEmpty();
                })
                .expectNext(1L)
                .verifyComplete();
            assertEquals(0, scan.scanUnsafe(Scannable.Attr.BUFFERED));
            handle.dispose();
            assertTrue(handle.isDisposed());
            StepVerifier.create(bus.publish("/buffer", "after-dispose")).expectNext(0L).verifyComplete();
            assertEquals(1, received.get());
        } finally { handle.dispose(); }
    }

    @Test
    public void streamingSubscriptionStillReportsItsRealQueueAndDrainsWithDemand() {
        var bus = new InternalEventBus();
        var source = bus.subscribe(Subscription.of("stream", "/buffer", Subscription.Feature.local));
        var scan = Scannable.from(source);
        StepVerifier.create(source, 0)
            .then(() -> {
                bus.publish("/buffer", "first").block();
                bus.publish("/buffer", "second").block();
                assertEquals(2, scan.scanUnsafe(Scannable.Attr.BUFFERED));
            })
            .thenRequest(2)
            .expectNextCount(2)
            .then(() -> assertEquals(0, scan.scanUnsafe(Scannable.Attr.BUFFERED)))
            .thenCancel()
            .verify();
        assertEquals(true, scan.scanUnsafe(Scannable.Attr.CANCELLED));
    }
}
