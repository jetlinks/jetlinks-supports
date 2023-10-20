package org.jetlinks.supports.event;

import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 事件生产者
 */
@Deprecated
public interface EventProducer extends EventConnection {

    /**
     * 发送订阅请求
     *
     * @param subscription 订阅请求
     */
    Mono<Void> subscribe(Subscription subscription);

    /**
     * 发送取消订阅请求
     *
     * @param subscription 订阅请求
     */
    Mono<Void> unsubscribe(Subscription subscription);

    /**
     * 从生产者订阅消息
     *
     * @return 消息流
     */
    Flux<TopicPayload> subscribe();

}
