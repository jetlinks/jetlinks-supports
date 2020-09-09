package org.jetlinks.supports.rpc;

import lombok.*;
import org.jetlinks.supports.event.BrokerEventBus;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class DefaultRpcServiceFactoryTest {


    @Test
    public void test() {
        DefaultRpcServiceFactory factory = new DefaultRpcServiceFactory(new EventBusRpcService(new BrokerEventBus()));


        TestService service = factory.createProducer("/test", TestService.class).getService();

        TestServiceConsumer consumer = new TestServiceConsumer();

        Disposable disposable = factory.createConsumer("/test", TestService.class, consumer);

        service.sayHello()
                .as(StepVerifier::create)
                .expectNext("hello")
                .verifyComplete();

        service.sayYeah()
                .as(StepVerifier::create)
                .expectComplete()
                .verify();

        service.genericNumber(4)
                .as(StepVerifier::create)
                .expectNext(0, 1, 2, 3)
                .verifyComplete();

        service.createList("1","2")
                .as(StepVerifier::create)
                .expectNext(Arrays.asList("1","2"))
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

            return Flux.range(0, numbers).delayElements(Duration.ofMillis(100));
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