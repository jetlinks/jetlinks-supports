package org.jetlinks.supports.server;

import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;

public interface ClientMessageHandler {
    Mono<Boolean> handleMessage(DeviceSession session, MessageDecodeContext message);
}
