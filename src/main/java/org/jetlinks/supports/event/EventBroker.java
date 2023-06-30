package org.jetlinks.supports.event;

import reactor.core.publisher.Flux;

/**
 * 事件代理
 *
 * @author zhouhao
 * @since 1.1.1
 */
@Deprecated
public interface EventBroker {

    /**
     * @return ID
     */
    String getId();

    /**
     * @return 从代理中接收事件连接
     */
    Flux<EventConnection> accept();

}
