package org.jetlinks.supports.ipc;

import lombok.SneakyThrows;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.junit.Assert.*;

public class RequestIdSupplierTest {

    @Test
    @SneakyThrows
    public void test() {
        RequestIdSupplier supplier = new RequestIdSupplier();

        Flux.range(0, 1000000)
            .publishOn(Schedulers.parallel())
            .map(i -> supplier.nextId((n) -> false))
            .distinct()
            .as(StepVerifier::create)
            .expectNextCount(1000000)
            .verifyComplete();

    }
}