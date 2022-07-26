package org.jetlinks.supports.things;

import org.jetlinks.core.things.ThingProperty;
import org.jetlinks.core.things.ThingType;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class LocalFileThingsDataManagerTest {

    @Test
    public void test2() {

        for (int i = 0; i < 100; i++) {
            test();
        }
    }

    @Test
    public void test() {
        LocalFileThingsDataManager dataManager = new LocalFileThingsDataManager("./target/thing-data.db");

        {
            long time = System.currentTimeMillis();
            Flux.range(0, 10000)
                .publishOn(Schedulers.single())
                .flatMap(i -> Flux
                        .range(0, 100)
                        .doOnNext(i1 -> {
                            dataManager.updateProperty("device",
                                                       "device-" + i,
                                                       "property-" + i1,
                                                       System.currentTimeMillis(),
                                                       ((long) i << 32) + i1, null);
                        })
                ).then()
                .as(StepVerifier::create)
                .expectComplete()
                .verify();
            System.out.println(System.currentTimeMillis() - time);

        }

        {
            long time = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                for (int i1 = 0; i1 < 10; i1++) {
                    dataManager
                            .getLastProperty(ThingType.of("device"), "device-" + i, "property-" + i1, System.currentTimeMillis())
                            .map(ThingProperty::getValue)
                            .as(StepVerifier::create)
                            .expectNext(((long) i << 32) + i1)
                            .verifyComplete();
                }
            }
            System.out.println(System.currentTimeMillis() - time);

        }


        dataManager.shutdown();

    }
}