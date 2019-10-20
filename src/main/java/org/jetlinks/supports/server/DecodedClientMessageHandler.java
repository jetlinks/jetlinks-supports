package org.jetlinks.supports.server;

import org.jetlinks.core.message.Message;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;

public interface DecodedClientMessageHandler {

    Mono<Boolean> handleMessage(DeviceSession session, Message message);


}
