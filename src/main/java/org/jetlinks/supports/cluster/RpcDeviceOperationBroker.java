package org.jetlinks.supports.cluster;

import com.google.common.cache.CacheBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.*;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.RpcService;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.core.utils.SerializeUtils;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.cache.RemovalCause.EXPIRED;

@Slf4j
public class RpcDeviceOperationBroker extends AbstractDeviceOperationBroker {

    private final RpcManager rpcManager;
    private final DeviceSessionManager sessionManager;

    private final Sinks.Many<Message> sendToDevice = Sinks
            .unsafe()
            .many()
            .unicast()
            .onBackpressureBuffer(Queues.<Message>unboundedMultiproducer().get());

    private final Map<String, RepayableDeviceMessage<?>> awaits = CacheBuilder
            .newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .<String, RepayableDeviceMessage<?>>removalListener(notify -> {
                if (notify.getCause() == EXPIRED) {
                    try {
                        RpcDeviceOperationBroker.log.debug("discard await reply message[{}] message,{}", notify.getKey(), notify.getValue());
                    } catch (Throwable ignore) {
                    }
                }
            })
            .build()
            .asMap();

    public RpcDeviceOperationBroker(RpcManager rpcManager, DeviceSessionManager sessionManager) {
        this.rpcManager = rpcManager;
        this.sessionManager = sessionManager;
        rpcManager.registerService(new ServiceImpl());
    }

    @Override
    public Flux<DeviceStateInfo> getDeviceState(String deviceGatewayServerId,
                                                Collection<String> deviceIdList) {
        return Flux
                .fromIterable(deviceIdList)
                .flatMap(id -> sessionManager
                        .checkAlive(id, false)
                        .map(alive -> new DeviceStateInfo(id, alive ? DeviceState.online : DeviceState.offline))
                );
    }

    @Override
    public Disposable handleGetDeviceState(String serverId,
                                           Function<Publisher<String>, Flux<DeviceStateInfo>> stateMapper) {
        return Disposables.disposed();
    }

    @Override
    public Flux<DeviceMessageReply> handleReply(String deviceId, String messageId, Duration timeout) {
        return super
                .handleReply(deviceId, messageId, timeout)
                .doOnCancel(()->{
                    //取消
                });
    }

    @Override
    public Mono<Integer> send(String deviceGatewayServerId, Publisher<? extends Message> message) {
        //发给同一个服务节点
        if (rpcManager.currentServerId().equals(deviceGatewayServerId)) {
            return Flux
                    .from(message)
                    .flatMap(this::handleSendToDevice)
                    .then(Reactors.ALWAYS_ONE);
        }

        return Flux
                .from(message)
                .flatMap(msg -> {
                    msg.addHeader(Headers.sendFrom, rpcManager.currentServerId());
                    addAwaitReplyKey(msg);
                    return rpcManager
                            .getService(deviceGatewayServerId, Service.class)
                            .flatMap(service -> service.send(encode(msg)).thenReturn(1))
                            .defaultIfEmpty(0);
                })
                .reduce(0, Integer::sum);
    }

    private Mono<Void> handleSendToDevice(Message message) {   
        addAwaitReplyKey(message);
        return doSendToDevice(message);
    }
    
      private void addAwaitReplyKey(Message message){
        if (message instanceof RepayableDeviceMessage && !message
                .getHeader(Headers.sendAndForget)
                .orElse(false)) {
            RepayableDeviceMessage<?> msg = ((RepayableDeviceMessage<?>) message);
            awaits.put(getAwaitReplyKey(msg), msg);
        }
    }

    private Mono<Void> doSendToDevice(Message message) {
        return TraceHolder
                .writeContextTo(message, Message::addHeader)
                .flatMap(msg -> {
                    if (sendToDevice.currentSubscriberCount() == 0) {
                        log.warn("no handler for message {}", msg);
                        return doReply(createReply(msg).error(ErrorCode.SYSTEM_ERROR));
                    }

                    try {
                        sendToDevice.emitNext(msg, Reactors.emitFailureHandler());
                    } catch (Throwable err) {
                        return doReply(createReply(msg).error(err));
                    }
                    return Mono.empty();
                });
    }

    private DeviceMessageReply createReply(Message message) {
        if (message instanceof RepayableDeviceMessage) {
            return ((RepayableDeviceMessage<?>) message).newReply();
        }
        if (message instanceof DeviceMessage) {
            return new CommonDeviceMessageReply<>()
                    .deviceId(((DeviceMessage) message).getDeviceId())
                    .messageId(message.getMessageId());
        }
        return new CommonDeviceMessageReply<>()
                .messageId(message.getMessageId());
    }


    @Override
    public Mono<Integer> send(Publisher<? extends BroadcastMessage> message) {
        return Reactors.ALWAYS_ZERO;
    }

    @Override
    public Flux<Message> handleSendToDeviceMessage(String serverId) {
        return sendToDevice.asFlux();
    }

    @Override
    protected Mono<Void> doReply(DeviceMessageReply reply) {
        RepayableDeviceMessage<?> msg = awaits.remove(getAwaitReplyKey(reply));
        String serviceId = null;

        if (null != msg) {
            serviceId = msg.getHeader(Headers.sendFrom).orElse(null);
        }
        Flux<Service> serviceFlux;
        if (serviceId != null) {
            serviceFlux = rpcManager
                    .getService(serviceId, Service.class)
                    .flux();
        } else {
            serviceFlux = rpcManager
                    .getServices(Service.class)
                    .map(RpcService::service);
        }
        return serviceFlux
                .flatMap(service -> service.reply(encode(reply)))
                .then();
    }

    @SneakyThrows
    protected ObjectInput createInput(ByteBuf input) {
        return new ObjectInputStream(new ByteBufInputStream(input, true));
    }

    @SneakyThrows
    protected ObjectOutput createOutput(ByteBuf output) {
        return new ObjectOutputStream(new ByteBufOutputStream(output));
    }

    static MessageType[] types = MessageType.values();

    @SneakyThrows
    private Message decode(ByteBuf buf) {
        try (ObjectInput input = createInput(buf)) {
            MessageType type = types[input.readByte()];
            Message msg = type.forDevice();
            if (msg != null) {
                msg.readExternal(input);
                return msg;
            }
            return (Message) SerializeUtils.readObject(input);
        }
    }

    @SneakyThrows
    private ByteBuf encode(Message message) {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        try (ObjectOutput output = createOutput(buf)) {
            output.writeByte(message.getMessageType().ordinal());
            if (message.getMessageType().iSupportDevice()) {
                message.writeExternal(output);
            } else {
                SerializeUtils.writeObject(message, output);
            }
            return buf;
        }
    }

    @io.scalecube.services.annotations.Service
    public interface Service {
        @ServiceMethod
        Mono<Void> send(ByteBuf payload);

        @ServiceMethod
        Mono<Void> reply(ByteBuf buf);
    }

    private class ServiceImpl implements Service {

        @Override
        public Mono<Void> send(ByteBuf payload) {
            return doSendToDevice(decode(payload));
        }

        @Override
        public Mono<Void> reply(ByteBuf buf) {
            Message msg = decode(buf);
            if (msg instanceof DeviceMessageReply) {
                handleReply(((DeviceMessageReply) msg));
            }
            return Mono.empty();

        }
    }
}
