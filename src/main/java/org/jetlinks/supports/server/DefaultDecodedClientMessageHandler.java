package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@Slf4j
public class DefaultDecodedClientMessageHandler implements DecodedClientMessageHandler {

    private MessageHandler deviceMessageBroker;

    private FluxProcessor<Message, Message> processor;

    public DefaultDecodedClientMessageHandler(MessageHandler handler) {
        this(handler, EmitterProcessor.create(false));
    }

    public DefaultDecodedClientMessageHandler(MessageHandler handler, FluxProcessor<Message, Message> processor) {
        this.deviceMessageBroker = handler;
        this.processor = processor;
        this.subscribe().flatMap(message -> {
            if (message instanceof DeviceMessageReply) {
                //强制回复
                if (message.getHeader(Headers.forceReply).orElse(false)) {
                    return doReply(((DeviceMessageReply) message));
                }
                if (!(message instanceof EventMessage)) {
                    return doReply(((DeviceMessageReply) message));
                }
            }
            return Mono.just(true);
        })
                .subscribe(success -> {

                });
    }

    public Flux<Message> subscribe() {
        return processor
                .map(Function.identity());
    }

    @Override
    public Mono<Boolean> handleMessage(DeviceSession session, Message message) {
        return Mono.fromSupplier(() -> {
            processor.onNext(message);
            return true;
        });

    }

    private Mono<Boolean> doReply(DeviceMessageReply reply) {
        if (log.isDebugEnabled()) {
            log.debug("reply message {}", reply.getMessageId());
        }
        return deviceMessageBroker
                .reply(reply)
                .doOnSuccess(success -> {
                    if (log.isDebugEnabled()) {
                        log.debug("reply message {} complete", reply.getMessageId());
                    }
                })
                .doOnError((error) -> log.error("reply message error", error));
    }
}
