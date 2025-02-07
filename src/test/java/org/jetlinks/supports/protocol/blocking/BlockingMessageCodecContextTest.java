package org.jetlinks.supports.protocol.blocking;

import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.monitor.Monitor;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class BlockingMessageCodecContextTest {


    @Test
    public void testAsyncError() {
        BlockingMessageCodecContext<MessageDecodeContext> context = new BlockingMessageCodecContext<>(
            Monitor.noop(),
            Mockito.mock(MessageDecodeContext.class),
            Duration.ofSeconds(10),
            Context.empty()
        );
        AtomicInteger ref = new AtomicInteger();

        context.async(
            Mono.fromRunnable(ref::incrementAndGet)
                .then(
                    Mono.fromRunnable(() -> {
                            context.async(Mono.error(new RuntimeException()));
                            context.async(Mono.fromRunnable(ref::incrementAndGet));
                        })
                        .subscribeOn(Schedulers.parallel())

                )
        );

        context
            .runAsync()
            .as(StepVerifier::create)
            .expectError(RuntimeException.class)
            .verify();

        assertEquals(1,ref.get());
    }
    @Test
    public void testAsync() {

        BlockingMessageCodecContext<MessageDecodeContext> context = new BlockingMessageCodecContext<>(
            Monitor.noop(),
            Mockito.mock(MessageDecodeContext.class),
            Duration.ofSeconds(10),
            Context.empty()
        );

        AtomicInteger ref = new AtomicInteger();

        context.async(
            Mono.fromRunnable(ref::incrementAndGet)
                .then(
                    Mono.fromRunnable(() -> {
                            context.async(Mono.fromRunnable(ref::incrementAndGet));
                        })
                        .subscribeOn(Schedulers.parallel())

                )
        );

        context
            .runAsync()
            .then(Mono.fromSupplier(ref::get))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();


    }

}