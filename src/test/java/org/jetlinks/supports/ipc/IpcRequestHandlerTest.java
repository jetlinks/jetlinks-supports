package org.jetlinks.supports.ipc;

import io.netty.util.ResourceLeakDetector;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class IpcRequestHandlerTest {
    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Test
    public void testOne() {
        IpcRequestHandler<Integer> handler = new IpcRequestHandler<>();

        handler.handleStream()
               .doOnSubscribe(sub -> {

                   handler.handle(IpcResponse.of(
                           ResponseType.complete ,
                           0,
                           0,
                           null,
                           null
                   ));
                   handler.handle(IpcResponse.of(
                           ResponseType.next ,
                           0,
                           0,
                           1,
                           null
                   ));


               })
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();
    }
    @Test
    public void test() {
        IpcRequestHandler<Integer> handler = new IpcRequestHandler<>();

        handler.handleStream()
               .doOnSubscribe(sub -> {
                   Flux.range(1, 100)
                       .publishOn(Schedulers.parallel())
                       .index()
                       .doOnNext(i -> {
                           handler.handle(IpcResponse.of(
                                   i.getT1() == 90 ? ResponseType.complete : ResponseType.next,
                                   i.getT1() == 90 ? 100 : i.getT1().intValue(),
                                   0,
                                   i.getT2(),
                                   null
                           ));
                       })
                       .subscribe();

               })
               .as(StepVerifier::create)
               .expectNextCount(100)
               .verifyComplete();

    }
}