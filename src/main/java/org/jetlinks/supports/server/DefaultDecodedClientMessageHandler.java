package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.DeviceSessionManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.extra.processor.TopicProcessor;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

@Slf4j
public class DefaultDecodedClientMessageHandler implements DecodedClientMessageHandler {

    private MessageHandler deviceMessageBroker;

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
        this.deviceMessageBroker = handler;
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

    protected Mono<Boolean> handleChildrenDeviceMessageReply(ChildDeviceMessageReply reply) {
        Message message = reply.getChildDeviceMessage();
        String deviceId = reply.getChildDeviceId();
        if (message instanceof DeviceMessageReply) {
            return handleDeviceMessageReply(((DeviceMessageReply) message));
        } else if (message instanceof DeviceOnlineMessage) {
            return sessionManager.registerChildren(reply.getDeviceId(), deviceId)
                    .map(__ -> true)
                    .switchIfEmpty(Mono.just(false));
        } else if (message instanceof DeviceOfflineMessage) {
            return sessionManager.unRegisterChildren(reply.getDeviceId(), deviceId)
                    .map(__ -> true)
                    .switchIfEmpty(Mono.just(false));
        }
        return Mono.just(true);
    }

    public void shutdown() {

    }

    public Flux<Message> subscribe() {
        return processor
                .map(Function.identity())
                .doOnError(err -> log.error(err.getMessage(), err));
    }

    @Override
    public Mono<Boolean> handleMessage(DeviceSession session, Message message) {
        return Mono.defer(() -> {
            if (message instanceof ChildDeviceMessageReply) {
                return handleChildrenDeviceMessageReply(((ChildDeviceMessageReply) message));
            }
            if (message instanceof DeviceMessageReply) {
                return handleDeviceMessageReply(((DeviceMessageReply) message));
            }
            return Mono.just(true);
        })
                .switchIfEmpty(Mono.just(false))
                .doOnError(err -> log.error("handle device[{}] message [{}] error", session.getDeviceId(), message, err))
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
        return deviceMessageBroker
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
