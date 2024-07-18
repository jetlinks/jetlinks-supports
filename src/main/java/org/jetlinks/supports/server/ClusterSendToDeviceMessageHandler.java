package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.device.DeviceOperationBroker;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.exception.DeviceOperationException;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.ToDeviceMessageContext;
import org.jetlinks.core.message.state.DeviceStateCheckMessage;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.ChildrenDeviceSession;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.LostDeviceSession;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class ClusterSendToDeviceMessageHandler {

    private static final HeaderKey<Boolean> resumeSession = HeaderKey.of("resume-session", true);

    private final DeviceSessionManager sessionManager;

    private final MessageHandler handler;

    private final DeviceRegistry registry;

    private final DecodedClientMessageHandler decodedClientMessageHandler;

    public ClusterSendToDeviceMessageHandler(DeviceSessionManager sessionManager,
                                             MessageHandler handler,
                                             DeviceRegistry registry,
                                             DecodedClientMessageHandler decodedClientMessageHandler) {
        this.sessionManager = sessionManager;
        this.handler = handler;
        this.registry = registry;
        this.decodedClientMessageHandler = decodedClientMessageHandler;
        init();
    }


    private void init() {
        handler
            .handleSendToDeviceMessage(
                sessionManager.getCurrentServerId(),
                this::handleMessage);
    }

    private DeviceMessageReply createReply(Message message) {
        DeviceMessageReply reply;
        if (message instanceof RepayableDeviceMessage) {
            reply = ((RepayableDeviceMessage<?>) message).newReply();
        } else {
            reply = new CommonDeviceMessageReply<>()
                .deviceId(((DeviceMessage) message).getDeviceId())
                .messageId(message.getMessageId());
        }
        return TraceHolder.copyContext(message.getHeaders(), reply, Message::addHeaderIfAbsent);
    }

    private Mono<Void> handleMessage(Message msg) {
        //异步发送
        if (msg.getHeaderOrDefault(Headers.async)) {
            return Mono.deferContextual(ctx -> {
                @SuppressWarnings("all")
                Disposable disposable = handleMessage0(msg)
                    .subscribe(null, null, null, Context.of(ctx));
                return Mono.empty();
            });
        }
        return handleMessage0(msg);
    }

    private Mono<Void> handleMessage0(Message msg) {
        if (!(msg instanceof DeviceMessage)) {
            return Mono.empty();
        }

        DeviceMessage message = ((DeviceMessage) msg);
        if (message.getDeviceId() == null) {
            log.warn("deviceId is null :{}", message);
            return Mono.empty();
        }
        return sessionManager
            .getSession(message.getDeviceId())
            //会话存在则直接发送给会话
            .map(session -> sendTo(session, message))
            //处理会话不存在的消息
            .defaultIfEmpty(Mono.defer(() -> sendToUnknownSession(message)))
            .flatMap(Function.identity())
            .contextWrite(ctx -> ctx.put(DeviceMessage.class, message));
    }

    @SuppressWarnings("all")
    private Mono<Void> sendTo(DeviceSession session, DeviceMessage message) {
        DeviceOperator device;
        //子设备会话都发给网关
        if (session.isWrapFrom(ChildrenDeviceSession.class)) {
            return sendToParentSession(session.getOperator(),
                                       session.unwrap(ChildrenDeviceSession.class).getParentDevice(),
                                       message);
        } else {
            device = session.getOperator();
        }

        if (session.isWrapFrom(LostDeviceSession.class)) {
            if (message instanceof DisconnectDeviceMessage) {
                return sessionManager
                    .remove(session.getDeviceId(), false)
                    .then(
                        doReply(device, ((DisconnectDeviceMessage) message).newReply().success())
                    );
            }
            return retryResume(device, message);
        }

        CodecContext context = new CodecContext(device, message, DeviceSession.trace(session));

        return Mono
            //交给会话处理
            .defer(() -> session.send(context))
            .filter(Boolean::booleanValue)
            //返回false或者empty,可能是不支持这类消息
            .switchIfEmpty(Mono.defer(() -> handleUnsupportedMessage(context).then(Mono.empty())))
            //发送消息异常
            .onErrorResume(err -> {
                if (!(err instanceof DeviceOperationException)) {
                    log.warn("handle send to device message error {}", context.message, err);
                }
                if (!context.alreadyReply) {
                    return this
                        .doReply(context, createReply(context.message).error(err))
                        .then(Mono.empty());
                }
                return Mono.empty();
            })
            .then(Mono.defer(() -> handleMessageSent(context)));

    }

    private Mono<Void> handleMessageSent(CodecContext context) {
        if (context.alreadyReply) {
            return Mono.empty();
        }
        //异步请求,直接回复已发送
        if (context.message.getHeader(Headers.async).orElse(false)) {
            return this
                .doReply(context, createReply(context.message)
                    .message(ErrorCode.REQUEST_HANDLING.getText())
                    .code(ErrorCode.REQUEST_HANDLING.name())
                    .success())
                .then();
        }
        return Mono.empty();
    }

    private Mono<Void> handleUnsupportedMessage(CodecContext context) {
        if (!context.alreadyReply) {
            //断开连接
            if (context.message instanceof DisconnectDeviceMessage) {
                return sessionManager
                    .remove(context.device.getDeviceId(), false)
                    .then(this.doReply(context, this.createReply(context.message).success()));
            }

            //子设备消息
            if (context.message instanceof ChildDeviceMessage) {
                ChildDeviceMessage child = ((ChildDeviceMessage) context.message);
                Message childMsg = child.getChildDeviceMessage();
                //断开子设备连接
                if (childMsg instanceof DisconnectDeviceMessage) {
                    return sessionManager
                        .remove(((DisconnectDeviceMessage) childMsg).getDeviceId(), false)
                        .then(this.doReply(context, this.createReply(context.message).success()));
                }
                //获取子设备状态
                if (childMsg instanceof DeviceStateCheckMessage) {
                    return this.doReply(context, this.createReply(context.message).success());
                }
            }
            //不支持
            return this.doReply(context, this.createReply(context.message).error(ErrorCode.UNSUPPORTED_MESSAGE));
        }

        return Mono.empty();

    }

    private Mono<Void> sendToUnknownSession(DeviceMessage message) {
        return registry
            .getDevice(message.getDeviceId())
            .flatMap(device -> device
                .getSelfConfig(DeviceConfigKey.parentGatewayId)
                .flatMap(registry::getDevice)
                //发给上级网关设备
                .map(parentDevice -> this.sendToParentSession(device, parentDevice, message))
                .defaultIfEmpty(Mono.defer(() -> sendToNoSession(device, message))))
            .flatMap(Function.identity());
    }

    private Mono<Void> sendToNoSession(DeviceOperator device, DeviceMessage message) {
        return sessionManager
            //检查整个集群的会话
            .checkAlive(message.getDeviceId(), false)
            .flatMap(exists -> {
                if (exists) {
                    log.warn("device session state failed,try resume. {}", message);
                    //会话依旧存在则尝试恢复发送
                    return retryResume(device, message);
                }
                //会话不存在,说明已经离线了
                boolean resume = message.getHeader(resumeSession).orElse(false);
                return doReply(device, createReply(message)
                    .addHeader("reason", "session_not_exists")
                    .error(resume ? ErrorCode.CONNECTION_LOST : ErrorCode.CLIENT_OFFLINE));
            });
    }

    private Mono<Void> retryResume(DeviceOperator device, DeviceMessage message) {
        //防止递归
        if (message.getHeader(resumeSession).isPresent()) {
            return doReply(device, createReply(message).error(ErrorCode.CONNECTION_LOST));
        }
        message.addHeader(resumeSession, true);
        //尝试发送给其他节点
        if (handler instanceof DeviceOperationBroker) {
            return device
                .getSelfConfig(DeviceConfigKey.connectionServerId)
                .flatMap(serverId -> ((DeviceOperationBroker) handler).send(serverId, Mono.just(message)))
                .flatMap(i -> {
                    if (i > 0) {
                        return Mono.empty();
                    }
                    return doReply(device, createReply(message).error(ErrorCode.CONNECTION_LOST));
                });
        }
        return doReply(device, createReply(message).error(ErrorCode.CONNECTION_LOST));
    }

    private Mono<Void> sendToParentSession(DeviceOperator device,
                                           DeviceOperator parent,
                                           DeviceMessage message) {

        ChildDeviceMessage child = new ChildDeviceMessage();
        child.setDeviceId(parent.getDeviceId());
        child.setChildDeviceId(device.getDeviceId());
        child.setChildDeviceMessage(message);
        child.setMessageId(message.getMessageId());
        Headers.copyFunctionalHeader(message, child);

        return handleMessage(child);
    }

    Mono<Void> doReply(DeviceOperator device, DeviceMessage message) {
        return decodedClientMessageHandler
            .handleMessage(device, message)
            .then();
    }

    Mono<Void> doReply(CodecContext context, DeviceMessage message) {
        if (context != null) {
            if (context.message.getHeader(Headers.sendAndForget).orElse(false)
                || context.alreadyReply) {
                return Mono.empty();
            }
            context.alreadyReply = true;
            return doReply(context.device, message)
                .contextWrite(ctx -> TraceHolder.readToContext(ctx, context.message.getHeaders()));
        }
        return doReply((DeviceOperator) null, message)
            .contextWrite(ctx -> TraceHolder.readToContext(ctx, message.getHeaders()));

    }

    class CodecContext implements ToDeviceMessageContext {
        private final DeviceOperator device;
        private final DeviceMessage message;
        private final DeviceSession session;

        private volatile boolean alreadyReply = false;

        CodecContext(DeviceOperator device, DeviceMessage message, DeviceSession session) {
            this.device = device;
            this.message = message;
            this.session = session;
        }

        @Nullable
        @Override
        public DeviceOperator getDevice() {
            return device;
        }

        @Override
        public Mono<DeviceOperator> getDevice(String deviceId) {
            return registry.getDevice(deviceId);
        }

        @Override
        public Map<String, Object> getConfiguration() {
            return ToDeviceMessageContext.super.getConfiguration();
        }

        @Override
        public Optional<Object> getConfig(String key) {
            return ToDeviceMessageContext.super.getConfig(key);
        }

        @Nonnull
        @Override
        public Message getMessage() {
            return message;
        }

        @Nonnull
        @Override
        public Mono<Void> reply(@Nonnull Publisher<? extends DeviceMessage> replyMessage) {
            alreadyReply = true;
            return Flux
                .from(replyMessage)
                //正常处理
                .map(msg -> decodedClientMessageHandler.handleMessage(device, msg).then())
                //空流
                .defaultIfEmpty(Mono.defer(() -> {
                    alreadyReply = false;
                    return Mono.empty();
                }))
                //回复流错误
                .onErrorResume(err -> {
                    alreadyReply = false;
                    return Mono.error(err);
                })
                .concatMap(Function.identity())
                .then();
        }

        @Override
        public Mono<Boolean> sendToDevice(@Nonnull EncodedMessage message) {
            //异步请求,只要发送则响应成功
            if (this.message.getHeaderOrDefault(Headers.async)) {
                return session
                    .send(message)
                    .flatMap(success -> success
                        ? handleMessageSent(this).then(Reactors.ALWAYS_TRUE)
                        //返回false,交给发送者处理.
                        : Reactors.ALWAYS_FALSE);
            }
            return session.send(message);
        }

        @Override
        public Mono<Void> disconnect() {
            return sessionManager
                .remove(device.getDeviceId(), true)
                .then();
        }

        @Nonnull
        @Override
        public DeviceSession getSession() {
            return session;
        }

        @Override
        public Mono<DeviceSession> getSession(String deviceId) {
            return sessionManager.getSession(deviceId);
        }

        @Override
        public Mono<Boolean> sessionIsAlive(String deviceId) {
            return sessionManager.isAlive(deviceId);
        }

        @Override
        public ToDeviceMessageContext mutate(Message anotherMessage, DeviceOperator device) {
            return new CodecContext(device, (DeviceMessage) anotherMessage, session);
        }
    }

}
