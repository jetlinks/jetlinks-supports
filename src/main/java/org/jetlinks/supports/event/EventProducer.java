package org.jetlinks.supports.event;

import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import reactor.core.publisher.Flux;

/**
 * 事件生产者
 */
public interface EventProducer extends EventConnection {

    /**
     * 发送订阅请求
     *
     * @param subscription 订阅请求
     */
    void subscribe(Subscription subscription);

    /**
     * 发送取消订阅请求
     *
     * @param subscription 订阅请求
     */
    void unsubscribe(Subscription subscription);

    /**
     *
     * @return
     */
    Flux<TopicPayload> subscribe();

}
