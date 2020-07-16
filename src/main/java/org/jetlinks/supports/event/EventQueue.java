package org.jetlinks.supports.event;

import org.jetlinks.core.Payload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventQueue {

    Flux<Payload> accept();

    Mono<Void> add(Payload payload);

    Mono<Void> add(Publisher<? extends Payload> payload);

}
