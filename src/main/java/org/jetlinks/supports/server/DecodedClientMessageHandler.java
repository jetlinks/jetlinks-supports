package org.jetlinks.supports.server;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.Message;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface DecodedClientMessageHandler {

    Mono<Boolean> handleMessage(@Nullable DeviceOperator device,@Nonnull Message message);


}
