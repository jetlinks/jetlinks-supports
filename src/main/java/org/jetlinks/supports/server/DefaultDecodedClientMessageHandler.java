package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.DeviceSessionManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.extra.processor.TopicProcessor;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

@Slf4j
public class DefaultDecodedClientMessageHandler implements DecodedClientMessageHandler {

    private MessageHandler messageHandler;

    private FluxProcessor<Message, Message> processor;

    private DeviceSessionManager sessionManager;

    public DefaultDecodedClientMessageHandler(MessageHandler handler, DeviceSessionManager sessionManager) {
        this(handler, sessionManager, TopicProcessor.<Message>builder()
                .bufferSize(128)
                .executor(ForkJoinPool.commonPool())
                .requestTaskExecutor(ForkJoinPool.commonPool())
                .share(true)
                .autoCancel(false)
                .build());
    }

    public DefaultDecodedClientMessageHandler(MessageHandler handler, DeviceSessionManager sessionManager, FluxProcessor<Message, Message> processor) {
        this.messageHandler = handler;
        this.processor = processor;
        this.sessionManager = sessionManager;
    }

    protected Mono<Boolean> handleDeviceMessageReply(DeviceMessageReply message) {
        //强制回复
        if (message.getHeader(Headers.forceReply).orElse(false)) {
            return doReply(message);
        }
        if (!(message instanceof EventMessage)) {
            return doReply(message);
        }
        return Mono.just(true);
    }

    protected Mono<Boolean> handleChildrenDeviceMessage(DeviceOperator session, String childrenId, Message message) {
        if (message instanceof DeviceMessageReply) {
            return handleDeviceMessageReply(((DeviceMessageReply) message));
        } else if (message instanceof DeviceOnlineMessage) {
            return sessionManager.registerChildren(session.getDeviceId(), childrenId)
                    .map(__ -> true)
                    .switchIfEmpty(Mono.just(false));
        } else if (message instanceof DeviceOfflineMessage) {
            return sessionManager.unRegisterChildren(session.getDeviceId(), childrenId)
                    .map(__ -> true)
                    .switchIfEmpty(Mono.just(false));
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
                    } else if (message instanceof DeviceOnlineMessage) {
                        return sessionManager.registerChildren(device.getDeviceId(), ((DeviceOnlineMessage) message).getDeviceId())
                                .map(__ -> true)
                                .switchIfEmpty(Mono.just(false));
                    } else if (message instanceof DeviceOfflineMessage) {
                        return sessionManager.unRegisterChildren(device.getDeviceId(), ((DeviceOfflineMessage) message).getDeviceId())
                                .map(__ -> true)
                                .switchIfEmpty(Mono.just(false));
                    }
                    if (message instanceof DeviceMessageReply) {
                        return handleDeviceMessageReply(((DeviceMessageReply) message));
                    }
                    return Mono.just(true);
                })
                .onErrorContinue((err, res) -> {
                    log.error("handle device[{}] message [{}] error", device.getDeviceId(), message, err);
                })
                .switchIfEmpty(Mono.just(false))
                .doFinally(s -> {
                    if (processor.hasDownstreams()) {
                        processor.onNext(message);
                    }
                });

    }

    private Mono<Boolean> doReply(DeviceMessageReply reply) {
        if (log.isDebugEnabled()) {
            log.debug("reply message {}", reply.getMessageId());
        }
        return messageHandler
                .reply(reply)
                .switchIfEmpty(Mono.just(false))
                .doOnSuccess(success -> {
                    if (log.isDebugEnabled()) {
                        log.debug("reply message {} complete", reply.getMessageId());
                    }
                })
                .doOnError((error) -> log.error("reply message error", error));
    }
}
