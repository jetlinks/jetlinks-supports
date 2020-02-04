package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.*;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.DeviceSessionManager;
import reactor.core.publisher.*;
import reactor.extra.processor.TopicProcessor;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

@Slf4j
public class DefaultDecodedClientMessageHandler implements DecodedClientMessageHandler {

    private MessageHandler messageHandler;

    private FluxProcessor<Message, Message> processor;

    private FluxSink<Message> sink;

    private DeviceSessionManager sessionManager;

    public DefaultDecodedClientMessageHandler(MessageHandler handler, DeviceSessionManager sessionManager) {
        this(handler, sessionManager, EmitterProcessor.create(false));
    }

    public DefaultDecodedClientMessageHandler(MessageHandler handler, DeviceSessionManager sessionManager, FluxProcessor<Message, Message> processor) {
        this.messageHandler = handler;
        this.processor = processor;
        this.sessionManager = sessionManager;
        this.sink = processor.sink();
    }


    protected Mono<Boolean> handleChildrenDeviceMessage(DeviceOperator session, String childrenId, Message message) {
        if (message instanceof DeviceMessageReply) {
            return doReply(((DeviceMessageReply) message));
        } else if (message instanceof DeviceOnlineMessage) {
            return sessionManager.registerChildren(session.getDeviceId(), childrenId)
                    .thenReturn(true)
                    .defaultIfEmpty(false);
        } else if (message instanceof DeviceOfflineMessage) {
            return sessionManager.unRegisterChildren(session.getDeviceId(), childrenId)
                    .thenReturn(true)
                    .defaultIfEmpty(false);
        }
        return Mono.just(true);
    }

    protected Mono<Boolean> handleChildrenDeviceMessageReply(DeviceOperator session, ChildDeviceMessage reply) {
        return handleChildrenDeviceMessage(session, reply.getChildDeviceId(), reply.getChildDeviceMessage());
    }

    protected Mono<Boolean> handleChildrenDeviceMessageReply(DeviceOperator session, ChildDeviceMessageReply reply) {
        return handleChildrenDeviceMessage(session, reply.getChildDeviceId(), reply.getChildDeviceMessage());
    }

    public void shutdown() {

    }

    public Flux<Message> subscribe() {
        return processor
                .map(Function.identity())
                .doOnError(err -> log.error(err.getMessage(), err));
    }

    @Override
    public Mono<Boolean> handleMessage(DeviceOperator device, Message message) {
        return Mono
                .defer(() -> {
                    if (message instanceof ChildDeviceMessageReply) {
                        return handleChildrenDeviceMessageReply(device, ((ChildDeviceMessageReply) message));
                    } else if (message instanceof ChildDeviceMessage) {
                        return handleChildrenDeviceMessageReply(device, ((ChildDeviceMessage) message));
                    }
                    if (message instanceof DeviceMessageReply) {
                        return doReply(((DeviceMessageReply) message));
                    }
                    return Mono.just(true);
                })
                .defaultIfEmpty(false)
                .doFinally(s -> {
                    if (processor.hasDownstreams()) {
                        sink.next(message);
                    }
                }).onErrorContinue((err, res) -> {
                    log.error("handle device[{}] message [{}] error", device.getDeviceId(), message, err);
                });

    }

    private Mono<Boolean> doReply(DeviceMessageReply reply) {
        if (log.isDebugEnabled()) {
            log.debug("reply message {}", reply.getMessageId());
        }
        return messageHandler
                .reply(reply)
                .doOnSuccess(success -> {
                    if (log.isDebugEnabled()) {
                        log.debug("reply message {} complete", reply.getMessageId());
                    }
                })
                .thenReturn(true)
                .doOnError((error) -> log.error("reply message error", error))
                ;
    }
}
