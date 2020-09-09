package org.jetlinks.supports.rpc;

import org.jetlinks.core.rpc.Invoker;
import org.jetlinks.core.rpc.RpcDefinition;
import org.jetlinks.supports.event.BrokerEventBus;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class EventBusRpcServiceTest {


    @Test
    public void test() {
        EventBusRpcService rcpService = new EventBusRpcService(new BrokerEventBus());

        RpcDefinition<String, String> definition = RpcDefinition.of("test", "/lower_case", String.class, String.class);

        Disposable disposable = rcpService.listen(definition, (addr, request) -> Mono.just(request.toLowerCase()));

        Invoker<String, String> invoker = rcpService.createInvoker(definition);

        invoker.invoke("HELLO")
                .as(StepVerifier::create)
                .expectNext("hello")
                .verifyComplete();

        disposable.dispose();

    }

    @Test
    public void testReturnFlux() {
        EventBusRpcService rcpService = new EventBusRpcService(new BrokerEventBus());

        RpcDefinition<Integer, Integer> definition = RpcDefinition.of("test", "/generic", Integer.class, Integer.class);

        Disposable disposable = rcpService.listen(definition, (addr, request) -> Flux.range(0, request).delayElements(Duration.ofMillis(100)));

        Invoker<Integer, Integer> invoker = rcpService.createInvoker(definition);

        invoker.invoke(10)
                .as(StepVerifier::create)
                .expectNextCount(10)
                .verifyComplete();

        invoker.invoke(Flux.just(4, 4, 4))
                .as(StepVerifier::create)
                .expectNextCount(12)
                .verifyComplete();

        disposable.dispose();

    }

}