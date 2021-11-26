package org.jetlinks.supports.event;

import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * 事件消费者
 *
 * @author zhouhao
 * @since 1.1.1
 */
public interface EventConsumer extends EventConnection {

    Flux<Subscription> handleSubscribe();

    Flux<Subscription> handleUnSubscribe();

    Sinks.Many<TopicPayload> sinksMany();

}
