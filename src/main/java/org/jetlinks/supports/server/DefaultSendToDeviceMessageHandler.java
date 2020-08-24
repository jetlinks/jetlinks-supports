package org.jetlinks.supports.server;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.*;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.exception.DeviceOperationException;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.ToDeviceMessageContext;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.ChildrenDeviceSession;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.DeviceSessionManager;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@AllArgsConstructor
public class DefaultSendToDeviceMessageHandler {

    private final String serverId;

    private final DeviceSessionManager sessionManager;

    private final MessageHandler handler;

    private final DeviceRegistry registry;

    private final DecodedClientMessageHandler decodedClientMessageHandler;

    public void startup() {

        //处理发往设备的消息
        handler.handleSendToDeviceMessage(serverId)
                .subscribe(message -> {
                    if (message instanceof DeviceMessage) {
                        handleDeviceMessage(((DeviceMessage) message));
                    }
                });

        //处理设备状态检查
        handler.handleGetDeviceState(serverId, deviceId -> Flux
                .from(deviceId)
                .map(id -> new DeviceStateInfo(id, sessionManager.sessionIsAlive(id) ? DeviceState.online : DeviceState.offline)));

    }

    protected void handleDeviceMessage(DeviceMessage message) {
        String deviceId = message.getDeviceId();
        DeviceSession session = sessionManager.getSession(deviceId);
        //在当前服务
        if (session != null) {
            doSend(message, session);
        } else {
            //判断子设备消息
            registry.getDevice(deviceId)
                    .flatMap(deviceOperator -> {
                        //获取上级设备
                        return deviceOperator
                                .getSelfConfig(DeviceConfigKey.parentGatewayId)
                                .flatMap(registry::getDevice);
                    })
                    .flatMap(operator -> {
                        ChildDeviceMessage children = new ChildDeviceMessage();
                        children.setDeviceId(operator.getDeviceId());
                        children.setMessageId(message.getMessageId());
                        children.setTimestamp(message.getTimestamp());
                        children.setChildDeviceId(deviceId);
                        children.setChildDeviceMessage(message);
                        // 没有传递header
                        // https://github.com/jetlinks/jetlinks-pro/issues/19
                        children.setHeaders(message.getHeaders());

                        ChildrenDeviceSession childrenDeviceSession = sessionManager.getSession(deviceId, operator.getDeviceId());
                        if (null != childrenDeviceSession) {
                            doSend(children, childrenDeviceSession);
                            return Mono.just(true);
                        }
                        DeviceSession childrenSession = sessionManager.getSession(operator.getDeviceId());
                        if (null != childrenSession) {
                            doSend(children, childrenSession);
                            return Mono.just(true);
                        }
                        //回复离线
                        return doReply(createReply(deviceId, message)
                                .error(ErrorCode.CLIENT_OFFLINE));
                    })
                    .switchIfEmpty(Mono.defer(() -> {
                        log.warn("device[{}] not connected,send message fail", message.getDeviceId());
                        return doReply(createReply(deviceId, message)
                                .error(ErrorCode.CLIENT_OFFLINE));
                    }))
                    .subscribe();

        }
    }

    protected DeviceMessageReply createReply(String deviceId, DeviceMessage message) {
        DeviceMessageReply reply;
        if (message instanceof RepayableDeviceMessage) {
            reply = ((RepayableDeviceMessage<?>) message).newReply();
        } else {
            reply = new CommonDeviceMessageReply<>();
        }
        reply.messageId(message.getMessageId()).deviceId(deviceId);
        return reply;
    }

    protected void doSend(DeviceMessage message, DeviceSession session) {
        String deviceId = message.getDeviceId();
        DeviceMessageReply reply = createReply(deviceId, message);
        AtomicBoolean alreadyReply = new AtomicBoolean(false);
        session.getOperator()
                .getProtocol()
                .flatMap(protocolSupport -> protocolSupport.getMessageCodec(session.getTransport()))
                .flatMapMany(codec -> codec.encode(new ToDeviceMessageContext() {
                    @Override
                    public Mono<Boolean> sendToDevice(@Nonnull EncodedMessage message) {
                        return session.send(message);
                    }

                    @Override
                    public Mono<Void> disconnect() {
                        return Mono.fromRunnable(() -> {
                            session.close();
                            sessionManager.unregister(session.getId());
                        });
                    }

                    @Nonnull
                    @Override
                    public DeviceSession getSession() {
                        return session;
                    }

                    @Nonnull
                    @Override
                    public Message getMessage() {
                        return message;
                    }

                    @Override
                    public DeviceOperator getDevice() {
                        return session.getOperator();
                    }

                    @Nonnull
                    @Override
                    public Mono<Void> reply(@Nonnull Publisher<? extends DeviceMessage> replyMessage) {
                        alreadyReply.set(true);
                        return Flux.from(replyMessage)
                                .flatMap(msg -> decodedClientMessageHandler.handleMessage(session.getOperator(), msg))
                                .then();
                    }
                }))
                .flatMap(session::send)
                .reduce((r1, r2) -> r1 && r2)
                .flatMap(success -> {
                    if (alreadyReply.get()) {
                        return Mono.empty();
                    }
                    if (message.getHeader(Headers.async).orElse(false)) {
                        return doReply(reply.message(ErrorCode.REQUEST_HANDLING.getText())
                                .code(ErrorCode.REQUEST_HANDLING.name())
                                .success());
                    }
                    return Mono.just(true);
                })
                .switchIfEmpty(Mono.defer(() -> {
                    //协议没处理断开连接消息
                    if (message instanceof DisconnectDeviceMessage) {
                        session.close();
                        sessionManager.unregister(session.getId());
                        return alreadyReply.get() ?
                                Mono.empty() :
                                doReply(createReply(deviceId, message).success());
                    } else {
                        return alreadyReply.get() ?
                                Mono.empty() :
                                doReply(createReply(deviceId, message)
                                        .error(ErrorCode.UNSUPPORTED_MESSAGE));
                    }
                }))
                .onErrorResume(error -> {
                    alreadyReply.set(true);
                    if (error instanceof DeviceOperationException) {
                        DeviceOperationException err = ((DeviceOperationException) error);
                        return doReply(reply.error(err.getCode()))
                                .onErrorContinue((e, res) -> log.error(e.getMessage(), e));
                    } else {
                        log.error(error.getMessage(), error);
                        return doReply(reply.error(error))
                                .onErrorContinue((e, res) -> log.error(e.getMessage(), e));
                    }
                })
                .subscribe();
    }


    private Mono<Boolean> doReply(DeviceMessageReply reply) {
        return handler
                .reply(reply)
                .as(mo -> {
                    if (log.isDebugEnabled()) {
                        return mo.doFinally(s -> log.debug("reply message {} ,[{}]", s, reply));
                    }
                    return mo;
                })
                .doOnError((error) -> log.error("reply message error", error));
    }

}
