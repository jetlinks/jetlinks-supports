package org.jetlinks.supports.server;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.message.codec.Transport;
import reactor.core.publisher.Mono;

public interface ClientMessageHandler {
    Mono<Boolean> handleMessage(DeviceOperator operator, Transport transport, MessageDecodeContext message);

}
