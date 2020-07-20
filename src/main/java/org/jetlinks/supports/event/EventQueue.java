package org.jetlinks.supports.event;

import org.jetlinks.core.event.TopicPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public interface EventQueue {

    Flux<TopicPayload> accept();

    void accept(FluxSink<TopicPayload> sink);

    Mono<Void> add(TopicPayload payload);

    Mono<Void> add(Publisher<TopicPayload> payload);

}
