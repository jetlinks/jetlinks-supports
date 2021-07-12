package org.jetlinks.supports.cache;

import lombok.SneakyThrows;
import org.jetlinks.core.cache.FileQueue;
import org.jetlinks.core.codec.defaults.StringCodec;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.test.StepVerifier;

import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MVStoreQueueBuilderFactoryTest {


    @Test
    @SneakyThrows
    public void test() {
//        MVStoreQueueBuilderFactory factory = new MVStoreQueueBuilderFactory();

        FileQueue<String> strings = FileQueue.<String>builder()
                .name("test")
                .path(Paths.get("./target/.queue"))
                .codec(StringCodec.UTF8)
                .build();
        int numberOf = 100_0000;
        long time = System.currentTimeMillis();
        Duration writeTime = Flux
                .range(0, numberOf)
                .doOnNext(i -> strings.add("data:" + i))
//                .flatMap(i -> Mono
//                        .fromCompletionStage(CompletableFuture.runAsync(() -> strings.add("data:" + i),Runnable::run)))
                .then()
                .as(StepVerifier::create)
                .verifyComplete();
        System.out.println("writeTime:" + writeTime);
        strings.flush();
        assertEquals(strings.size(), numberOf);

        Duration pollTime = Flux
                .range(0, numberOf)
                .map(i -> strings.poll())
//                .flatMap(i -> {
//                    return Mono.fromCompletionStage(CompletableFuture.supplyAsync(strings::poll, Runnable::run));
//                })
                .as(StepVerifier::create)
                .expectNextCount(numberOf)
                .verifyComplete();
        System.out.println("pollTime:" + pollTime);

        assertTrue(strings.isEmpty());

        System.out.println(System.currentTimeMillis() - time);
        strings.flush();
        strings.close();
        Thread.sleep(10000);

    }

    @Test
    public void testFlux() {
        FluxProcessor<String, String> processor = FileQueue
                .<String>builder()
                .name("test-flux")
                .path(Paths.get("./target/.queue"))
                .codec(StringCodec.UTF8)
                .buildFluxProcessor();

        processor.onNext("test");
        processor
                .take(1)
                .as(StepVerifier::create)
                .expectNext("test")
                .verifyComplete();

    }
}