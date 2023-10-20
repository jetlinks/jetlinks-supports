package org.jetlinks.supports.event;

import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/**
 * 事件消费者
 *
 * @author zhouhao
 * @since 1.1.1
 */
@Deprecated
public interface EventConsumer extends EventConnection {

    Flux<Subscription> handleSubscribe();

    Flux<Subscription> handleUnSubscribe();

    FluxSink<TopicPayload> sink();

}
