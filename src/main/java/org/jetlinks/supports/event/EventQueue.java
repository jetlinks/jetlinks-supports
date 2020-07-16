package org.jetlinks.supports.event;

import org.jetlinks.core.Payload;
import org.jetlinks.core.event.TopicPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public interface EventQueue {

    Flux<TopicPayload> accept();

    void accept(FluxSink<TopicPayload> sink);

    Mono<Void> add(Payload payload);

    Mono<Void> add(Publisher<? extends Payload> payload);

}
