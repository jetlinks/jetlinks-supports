package org.jetlinks.supports.event;

import org.jetlinks.core.codec.Decoder;
import org.jetlinks.core.codec.Encoder;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class CompositeEventBus implements EventBus {

    private final List<EventBus> eventBuses = new CopyOnWriteArrayList<>();

    public void addEventBus(EventBus bus) {
        eventBuses.add(bus);
    }

    @Override
    public @NotNull Flux<TopicPayload> subscribe(@NotNull Subscription subscription) {
        return Flux.fromIterable(eventBuses)
                .map(eventBus -> eventBus.subscribe(subscription))
                .as(Flux::merge)
                ;
    }

    @Override
    public @NotNull <T> Flux<T> subscribe(@NotNull Subscription subscription, @NotNull Decoder<T> type) {
        return Flux.fromIterable(eventBuses)
                .map(eventBus -> eventBus.subscribe(subscription, type))
                .as(Flux::merge)
                ;
    }

    @Override
    public @NotNull <T> Mono<Integer> publish(@NotNull String topic, @NotNull Publisher<T> event) {
        return Flux.fromIterable(eventBuses)
                .map(eventBus -> eventBus.publish(topic, event))
                .as(Flux::merge)
                .reduce(Math::addExact)
                ;
    }

    @Override
    public @NotNull <T> Mono<Integer> publish(@NotNull String topic, @NotNull Encoder<T> encoder, @NotNull Publisher<? extends T> eventStream) {
        return Flux.fromIterable(eventBuses)
                .map(eventBus -> eventBus.publish(topic, encoder, eventStream))
                .as(Flux::merge)
                .reduce(Math::addExact);
    }
}
