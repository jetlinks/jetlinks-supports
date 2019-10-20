package org.jetlinks.supports.server;

import lombok.AllArgsConstructor;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;

@AllArgsConstructor
public class DefaultClientMessageHandler implements ClientMessageHandler {

    private DecodedClientMessageHandler messageHandler;

    @Override
    public Mono<Boolean> handleMessage(DeviceSession session, MessageDecodeContext message) {

        return session
                .getOperator()
                .getProtocol()
                .flatMap(protocolSupport -> protocolSupport.getMessageCodec(session.getTransport()))
                .<Message>flatMap(codec -> codec.decode(message))
                .flatMap(msg -> messageHandler.handleMessage(session, msg));
    }
}
