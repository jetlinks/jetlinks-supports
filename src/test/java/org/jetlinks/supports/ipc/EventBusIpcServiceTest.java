package org.jetlinks.supports.ipc;

import io.netty.util.ResourceLeakDetector;
import org.jetlinks.core.ipc.IpcDefinition;
import org.jetlinks.core.ipc.IpcInvoker;
import org.jetlinks.core.ipc.IpcInvokerBuilder;
import org.jetlinks.supports.event.BrokerEventBus;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class EventBusIpcServiceTest {
    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Test
    public void testRequest() {
        doTest(
                IpcDefinition.of("lowercase", String.class, String.class),
                IpcInvokerBuilder.forRequest("invoke", str -> Mono.just(str.toLowerCase())),
                consumer -> consumer
                        .request("TEST")
                        .as(StepVerifier::create)
                        .expectNext("test")
                        .verifyComplete()
        );
    }

    @Test
    public void testRequestNoArg() {
        doTest(
                IpcDefinition.of("lowercase", String.class, String.class),
                IpcInvokerBuilder.forRequest("invoke", () -> Mono.just("hello")),
                consumer -> consumer
                        .request()
                        .as(StepVerifier::create)
                        .expectNext("hello")
                        .verifyComplete()
        );
    }

    @Test
    public void testRequestStreamNoArg() {
        doTest(
                IpcDefinition.of("lowercase", String.class, String.class),
                IpcInvokerBuilder.forRequestStream("invoke", () -> Flux.just("a", "b", "c", "d")),
                consumer -> consumer
                        .requestStream()
                        .as(StepVerifier::create)
                        .expectNext("a", "b", "c", "d")
                        .verifyComplete()
        );
    }

    @Test
    public void testRequestStream() {
        doTest(
                IpcDefinition.of("generateNumber", Integer.class, Integer.class),
                IpcInvokerBuilder.forRequestStream("invoke", (i) -> Flux.range(0, i)),
                consumer -> consumer
                        .requestStream(10000)
                        .as(StepVerifier::create)
                        .expectNextCount(10000)
                        .verifyComplete()
        );
    }


    @Test
    public void testRequestChannel() {
        doTest(
                IpcDefinition.of("channel", Integer.class, Integer.class),
                IpcInvokerBuilder.forRequestChannel("invoke", (stream) -> Flux.from(stream)),
                consumer -> consumer
                        .requestChannel(Flux.range(0, 100))
                        .as(StepVerifier::create)
                        .expectNextCount(100)
                        .verifyComplete()
        );
    }


    @Test
    public void testFireAndForget() {
        AtomicReference<Integer> ref = new AtomicReference<>();

        doTest(
                IpcDefinition.of("channel", Integer.class, Integer.class),
                IpcInvokerBuilder.forFireAndForget("invoke", (in) -> Mono.fromRunnable(() -> ref.set(in))),
                consumer -> consumer
                        .fireAndForget(100)
                        .then(Mono.fromSupplier(ref::get))
                        .as(StepVerifier::create)
                        .expectNext(100)
                        .verifyComplete()
        );
    }

    @Test
    public void testFireAndForgetNoArg() {
        AtomicReference<Integer> ref = new AtomicReference<>();

        doTest(
                IpcDefinition.of("channel", Integer.class, Integer.class),
                IpcInvokerBuilder.forFireAndForget("invoke", () -> Mono.fromRunnable(() -> ref.set(100))),
                consumer -> consumer
                        .fireAndForget()
                        .then(Mono.fromSupplier(ref::get))
                        .as(StepVerifier::create)
                        .expectNext(100)
                        .verifyComplete()
        );
    }

    public <REQ, RES> void doTest(IpcDefinition<REQ, RES> definition, IpcInvoker<REQ, RES> invoker, Consumer<IpcInvoker<REQ, RES>> consumer) {
        BrokerEventBus eventBus = new BrokerEventBus();
        eventBus.setPublishScheduler(Schedulers.parallel());
        EventBusIpcService service = new EventBusIpcService(1, eventBus);

        EventBusIpcService service2 = new EventBusIpcService(2, eventBus);

        service.listen(definition, invoker);

       // service2.listen(definition, invoker);

        consumer.accept(service2.createInvoker("invoke", definition));
    }
}