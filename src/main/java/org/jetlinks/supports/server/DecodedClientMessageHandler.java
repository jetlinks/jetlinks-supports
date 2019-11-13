package org.jetlinks.supports.server;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.Message;
import reactor.core.publisher.Mono;

public interface DecodedClientMessageHandler {

    Mono<Boolean> handleMessage(DeviceOperator device, Message message);


}
