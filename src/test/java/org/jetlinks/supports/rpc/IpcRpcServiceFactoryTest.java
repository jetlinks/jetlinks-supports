package org.jetlinks.supports.rpc;

import io.netty.util.ResourceLeakDetector;
import lombok.*;
import org.jetlinks.supports.event.BrokerEventBus;
import org.jetlinks.supports.ipc.EventBusIpcService;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

public class IpcRpcServiceFactoryTest {
    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }
    @Test
    @SneakyThrows
    public void test() {

        BrokerEventBus eventBus= new BrokerEventBus();
        eventBus.setPublishScheduler(Schedulers.parallel());
        IpcRpcServiceFactory factory = new IpcRpcServiceFactory(new EventBusIpcService(1, eventBus));

        IpcRpcServiceFactory factory2 = new IpcRpcServiceFactory(new EventBusIpcService(2, eventBus));


        TestService service = factory.createProducer("test", TestService.class).getService();

        TestServiceConsumer consumer = new TestServiceConsumer();

        Disposable disposable = factory2.createConsumer("test", TestService.class, consumer);

        service.sayHello()
                .as(StepVerifier::create)
                .expectNext("hello")
                .verifyComplete();

        service.sayYeah()
                .as(StepVerifier::create)
                .expectComplete()
                .verify();
//
        service.genericNumber(40)
                .as(StepVerifier::create)
                .expectNextCount(40)
                .verifyComplete();

        service.createList("1","2")
               .as(StepVerifier::create)
               .expectNext(Arrays.asList("1", "2"))
               .verifyComplete();

        service.getObjs()
               .as(StepVerifier::create)
               .expectNext(Arrays.asList(new TestObj("test")))
               .verifyComplete();


        service.getBoolean(true)
               .as(StepVerifier::create)
               .expectNext(true)
               .verifyComplete();

        service.getBoolean(false)
               .as(StepVerifier::create)
               .expectNext(false)
               .verifyComplete();


        disposable.dispose();
//        Thread.sleep(100000);
    }


    public interface TestService {
        Mono<String> sayHello();

        Mono<Void> sayYeah();

        Mono<List<String>> createList(String... args);

        Mono<List<TestObj>> getObjs();

        Flux<Integer> genericNumber(int numbers);

        Mono<Boolean> getBoolean(boolean numbers);
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TestObj{
        private String id;
    }

    public class TestServiceConsumer implements TestService {
        @Override
        public Flux<Integer> genericNumber(int numbers) {

            return Flux.range(0, numbers);
        }

        @Override
        public Mono<Boolean> getBoolean(boolean bool) {
            return Mono.just(bool);
        }

        @Override
        public Mono<String> sayHello() {

            return Mono.justOrEmpty("hello");
        }

        @Override
        public Mono<Void> sayYeah() {
            System.out.println("yeah");
            return Mono.empty();
        }

        @Override
        public Mono<List<String>> createList(String... args) {
            return Flux.fromArray(args)
                       .collectList();
        }

        @Override
        public Mono<List<TestObj>> getObjs() {
            return Mono.just(Arrays.asList(new TestObj("test")));
        }
    }

}