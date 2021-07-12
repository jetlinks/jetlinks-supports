package org.jetlinks.supports.cache;

import org.jetlinks.core.cache.FileQueue;
import org.jetlinks.core.codec.defaults.StringCodec;
import org.junit.Test;
import reactor.core.publisher.FluxProcessor;
import reactor.test.StepVerifier;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MVStoreQueueBuilderFactoryTest {


    @Test
    public void test() {
//        MVStoreQueueBuilderFactory factory = new MVStoreQueueBuilderFactory();

        FileQueue<String> strings = FileQueue.<String>builder()
                .name("test")
                .path(Paths.get("./target/.queue"))
                .codec(StringCodec.UTF8)
                .build();

        long time = System.currentTimeMillis();
        for (int i = 0; i < 10_0000; i++) {
            strings.add("data:" + i);
        }

        assertEquals(strings.size(), 10_0000);


        for (int i = 0; i < 10_0000; i++) {
            assertEquals(strings.poll(), "data:" + i);
        }

        assertTrue(strings.isEmpty());

        System.out.println(System.currentTimeMillis() - time);
        strings.close();

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