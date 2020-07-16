package org.jetlinks.supports.event;

import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import reactor.core.publisher.Flux;

/**
 * 事件生产者
 */
public interface EventProducer extends EventConnection {

    void subscribe(Subscription subscription);

    void unsubscribe(Subscription subscription);

    Flux<TopicPayload> subscribe();

}
