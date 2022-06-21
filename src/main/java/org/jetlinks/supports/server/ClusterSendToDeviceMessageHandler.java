package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.ToDeviceMessageContext;
import org.jetlinks.core.message.state.DeviceStateCheckMessage;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.ChildrenDeviceSession;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.trace.DeviceTracer;
import org.jetlinks.core.trace.TraceHolder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.jetlinks.core.trace.FluxTracer.create;

@Slf4j
public class ClusterSendToDeviceMessageHandler {

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
                .handleSendToDeviceMessage(sessionManager.getCurrentServerId())
                .flatMap(msg -> handleMessage(msg)
                        .onErrorResume(err -> {
                            log.error("handle send to device message error {}", msg, err);
                            return Mono.empty();
                        }))
                .subscribe();
    }

    private DeviceMessageReply createReply(Message message) {
        if (message instanceof RepayableDeviceMessage) {
            return ((RepayableDeviceMessage<?>) message).newReply();
        }
        return new CommonDeviceMessageReply<>()
                .deviceId(((DeviceMessage) message).getDeviceId())
                .messageId(message.getMessageId());
    }

    private Mono<Void> handleMessage(Message msg) {
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
                .contextWrite(TraceHolder.readToContext(Context.empty(), message.getHeaders()));
    }

    private Mono<Void> sendTo(DeviceSession session, DeviceMessage message) {
        DeviceOperator device;
        //子设备会话都发给网关
        if (session.isWrapFrom(ChildrenDeviceSession.class)) {
            device = session.unwrap(ChildrenDeviceSession.class).getParentDevice();
            if (!(message instanceof ChildDeviceMessage)) {
                ChildDeviceMessage msg = new ChildDeviceMessage();
                msg.setChildDeviceMessage(message);
                msg.setChildDeviceId(message.getDeviceId());
                msg.setDeviceId(device.getDeviceId());
                message = msg;
            }
        } else {
            device = session.getOperator();
        }
        //never happen
        if (null == device) {
            return this
                    .doReply((DeviceOperator) null, createReply(message).error(ErrorCode.CONNECTION_LOST))
                    .then();
        }
        CodecContext context = new CodecContext(device, message, DeviceSession.trace(session));

        return device
                .getProtocol()
                .flatMap(protocol -> protocol.getMessageCodec(context.session.getTransport()))
                .flatMapMany(codec -> codec.encode(context))
                .as(create(DeviceTracer.SpanName.encode(device.getDeviceId()),
                           (span, msg) -> span.setAttribute(DeviceTracer.SpanKey.message, msg.toString())))
                //发送给会话
                .map(msg -> context.session.send(msg).then())
                //协议包返回了empty,可能是不支持这类消息
                .defaultIfEmpty(Mono.defer(() -> handleUnsupportedMessage(context)))
                .flatMap(Function.identity())
                .onErrorResume(err -> {
                    log.error("handle send to device message error {}", context.message, err);
                    if (!context.alreadyReply) {
                        return doReply(context, createReply(context.message).error(err));
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
                        .defaultIfEmpty(Mono.defer(() -> doReply(device, createReply(message).error(ErrorCode.CLIENT_OFFLINE)))))
                .flatMap(Function.identity());
    }

    private Mono<Void> sendToParentSession(DeviceOperator device,
                                           DeviceOperator parent,
                                           DeviceMessage message) {

        ChildDeviceMessage child = new ChildDeviceMessage();
        child.setDeviceId(parent.getDeviceId());
        child.setChildDeviceId(device.getDeviceId());
        child.setChildDeviceMessage(message);
        child.setMessageId(message.getMessageId());
        child.setHeaders(new ConcurrentHashMap<>(message.getHeaders()));

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
        }
        return doReply(context == null ? null : context.device, message)
                .then();
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
                    .flatMap(msg -> decodedClientMessageHandler.handleMessage(device, msg))
                    .then();
        }

        @Override
        public Mono<Boolean> sendToDevice(@Nonnull EncodedMessage message) {
            return session.send(message);
        }

        @Override
        public Mono<Void> disconnect() {
            session.close();

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
    }

}
