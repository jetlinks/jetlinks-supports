package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceMessageHandler;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@Slf4j
public class DefaultDecodedClientMessageHandler implements DecodedClientMessageHandler {

    private DeviceMessageHandler deviceMessageHandler;

    private EmitterProcessor<Message> processor = EmitterProcessor.create(false);

    public DefaultDecodedClientMessageHandler(DeviceMessageHandler handler) {
        this.deviceMessageHandler = handler;
    }

    public Flux<Message> subscrube() {
        return processor.map(Function.identity());
    }

    @Override
    public Mono<Boolean> handleMessage(DeviceSession session, Message message) {
        return Mono.defer(() -> {
            if (processor.hasDownstreams()) {
                processor.onNext(message);
            }
            if (message instanceof DeviceMessageReply) {
                //强制回复
                if (message.getHeader(Headers.forceReply).orElse(false)) {
                    return doReply(((DeviceMessageReply) message));
                }
                if (!(message instanceof EventMessage)) {
                    return doReply(((DeviceMessageReply) message));
                }
            }

            return Mono.just(false);
        });
    }

    private Mono<Boolean> doReply(DeviceMessageReply reply) {
        return deviceMessageHandler
                .reply(reply)
                .doOnError((error) -> log.error("回复设备消息失败:{}", reply, error));
    }
}
