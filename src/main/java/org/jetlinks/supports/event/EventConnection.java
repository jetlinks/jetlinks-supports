package org.jetlinks.supports.event;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public interface EventConnection {

    String getId();

    boolean isAlive();

    void doOnDispose(Disposable disposable);

    default boolean isProducer() {
        return this instanceof EventProducer;
    }

    default boolean isConsumer() {
        return this instanceof EventConsumer;
    }

    default Mono<EventProducer> asProducer() {
        return isProducer() ? Mono.just(this).cast(EventProducer.class) : Mono.empty();
    }

    default Mono<EventConsumer> asConsumer() {
        return isConsumer() ? Mono.just(this).cast(EventConsumer.class) : Mono.empty();
    }
}
