package org.jetlinks.supports.server;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.Message;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 解码后设备消息处理器,用于处理解码后的设备消息.
 *
 * @author zhouhao
 * @since 1.0
 */
public interface DecodedClientMessageHandler {

    /**
     * 处理消息
     * @param device 设备操作接口,可以为null
     * @param message
     * @return
     */
    Mono<Boolean> handleMessage(@Nullable DeviceOperator device, @Nonnull Message message);


}
