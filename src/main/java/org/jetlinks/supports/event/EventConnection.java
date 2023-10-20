package org.jetlinks.supports.event;

import org.hswebframework.web.dict.EnumDict;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * 事件连接
 *
 * @author zhouhao
 * @since 1.1.1
 */
@Deprecated
public interface EventConnection extends Disposable {

    String getId();

    boolean isAlive();

    void doOnDispose(Disposable disposable);

    EventBroker getBroker();

    default Feature[] features() {
        return new Feature[0];
    }

    /**
     * @return 是否为事件生产者
     */
    default boolean isProducer() {
        return this instanceof EventProducer;
    }

    /**
     * @return 是否为事件消费者
     */
    default boolean isConsumer() {
        return this instanceof EventConsumer;
    }

    /**
     * @return 转为事件生产者
     */
    default Mono<EventProducer> asProducer() {
        return isProducer() ? Mono.just(this).cast(EventProducer.class) : Mono.empty();
    }

    /**
     * @return 转为事件消费者
     */
    default Mono<EventConsumer> asConsumer() {
        return isConsumer() ? Mono.just(this).cast(EventConsumer.class) : Mono.empty();
    }

    enum Feature implements EnumDict<String> {
        //消费同一个broker的消息
        consumeSameBroker,
        //消费同一个connection的消息
        consumeSameConnection,
        //订阅其他broker的消息
        consumeAnotherBroker,
        ;

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getText() {
            return name();
        }
    }
}
