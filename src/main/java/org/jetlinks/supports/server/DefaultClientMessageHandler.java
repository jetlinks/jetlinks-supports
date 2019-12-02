package org.jetlinks.supports.server;

import lombok.AllArgsConstructor;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.message.codec.Transport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@AllArgsConstructor
public class DefaultClientMessageHandler implements ClientMessageHandler {

    private DecodedClientMessageHandler messageHandler;

    @Override
    public Mono<Boolean> handleMessage(DeviceOperator operator, Transport transport, MessageDecodeContext message) {
        return operator
                .getProtocol()
                .flatMap(protocolSupport -> protocolSupport.getMessageCodec(transport))
                .<Message>flatMapMany(codec -> Flux.from(codec.decode(message)))
                .flatMap(msg -> messageHandler.handleMessage(operator, msg))
                .all(success -> success);
    }
}
