package org.jetlinks.supports.cluster;

import com.google.common.cache.CacheBuilder;
import io.netty.buffer.*;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.device.session.DeviceSessionInfo;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.*;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.RpcService;
import org.jetlinks.core.server.session.DeviceSessionSelector;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.core.utils.SerializeUtils;
import org.reactivestreams.Publisher;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
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

    private final Map<AwaitKey, RepayableDeviceMessage<?>> awaits = CacheBuilder
        .newBuilder()
        .expireAfterWrite(Duration.ofMinutes(5))
        .<AwaitKey, RepayableDeviceMessage<?>>removalListener(notify -> {
            if (notify.getCause() == EXPIRED) {
                try {
                    RpcDeviceOperationBroker.log.debug("discard await reply message[{}] message,{}", notify.getKey(), notify.getValue());
                } catch (Throwable ignore) {
                }
            }
        })
        .build()
        .asMap();

    private final List<Function<Message, Mono<Integer>>> handler = new CopyOnWriteArrayList<>();

    public RpcDeviceOperationBroker(RpcManager rpcManager, DeviceSessionManager sessionManager) {
        this.rpcManager = rpcManager;
        this.sessionManager = sessionManager;
        rpcManager.registerService(new ServiceImpl());
        Schedulers
            .parallel()
            .schedulePeriodically(
                this::checkExpires,
                10,
                10,
                TimeUnit.SECONDS);
    }

    @Override
    public Flux<DeviceStateInfo> getDeviceState(String deviceGatewayServerId,
                                                Collection<String> deviceIdList) {
        if (deviceIdList.size() == 1) {
            String deviceId = deviceIdList.iterator().next();
            return sessionManager
                .checkAlive(deviceId, false)
                .map(alive -> new DeviceStateInfo(
                    deviceId,
                    alive ? DeviceState.online : DeviceState.offline))
                .flux();
        }
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


    public Mono<Integer> send(String deviceGatewayServerId, DeviceMessage msg) {
        msg.addHeader(Headers.sendFrom, rpcManager.currentServerId());
        // 指定了服务ID
        if (StringUtils.hasText(deviceGatewayServerId)) {
            // 本地节点
            if (Objects.equals(deviceGatewayServerId, rpcManager.currentServerId())) {
                return doSendToDevice(msg);
            }
            // 发送给指定的节点
            ByteBuf buf = encode(msg);
            ByteBuf unreleasableBuffer = Unpooled.unreleasableBuffer(buf);
            return rpcManager
                .getService(deviceGatewayServerId, Service.class)
                .flatMap(_service -> _service.send(unreleasableBuffer))
                .switchIfEmpty(Reactors.ALWAYS_ZERO)
                .doFinally(ignore -> ReferenceCountUtil.release(buf));
        }

        byte selector = msg.getHeaderOrDefault(Headers.sessionSelector);
        // 发送给全部的节点
        if (selector == DeviceSessionSelector.all) {
            ByteBuf buf = encode(msg);
            ByteBuf unreleasableBuffer = Unpooled.unreleasableBuffer(buf);
            return Flux
                .concat(
                    doSendToDevice(msg),
                    rpcManager
                        .getServices(Service.class)
                        .map(RpcService::service)
                        .concatMap(_service -> _service.send(unreleasableBuffer))
                )
                .reduce(0, Math::addExact)
                .doFinally(ignore -> ReferenceCountUtil.release(buf));
        }
        // 最新的节点
        else if (selector == DeviceSessionSelector.latest) {
            return sessionManager
                .getDeviceSessionInfo(msg.getDeviceId())
                .reduce(DeviceSessionSelector.SESSION_INFO_LATEST)
                .flatMap(info -> sendTo(msg, Flux.just(info), Flux::singleOrEmpty))
                .switchIfEmpty(Reactors.ALWAYS_ZERO);
        }
        // 最旧的节点
        else if (selector == DeviceSessionSelector.oldest) {
            return sessionManager
                .getDeviceSessionInfo(msg.getDeviceId())
                .reduce(DeviceSessionSelector.SESSION_INFO_OLDEST)
                .flatMap(info -> sendTo(msg, Flux.just(info), Flux::singleOrEmpty))
                .switchIfEmpty(Reactors.ALWAYS_ZERO);
        }
        // hash方式
        else if (selector == DeviceSessionSelector.hashed) {
            return sessionManager
                .getDeviceSessionInfo(msg.getDeviceId())
                .reduce((DeviceSessionSelector.hashedOperator(DeviceSessionInfo::getServerId, msg)))
                .flatMap(info -> sendTo(msg, Flux.just(info), Flux::singleOrEmpty))
                .switchIfEmpty(Reactors.ALWAYS_ZERO);
        }
        // minimumLoad
        else if (selector == DeviceSessionSelector.minimumLoad) {
            return sessionManager
                .getDeviceSessionInfo(msg.getDeviceId())
                .reduce(DeviceSessionSelector.SESSION_INFO_MINIMUM_LOAD)
                .flatMap(info -> sendTo(msg, Flux.just(info), Flux::singleOrEmpty))
                .switchIfEmpty(Reactors.ALWAYS_ZERO);
        }

        // 选择任意一个节点
        return sendTo(
            msg,
            sessionManager.getDeviceSessionInfo(msg.getDeviceId()),
            flux -> flux.filter(i -> i > 0).take(1).singleOrEmpty());
    }

    private Mono<Integer> sendTo(DeviceMessage msg, Flux<DeviceSessionInfo> receiver, Function<Flux<Integer>, Mono<Integer>> reducer) {
        // 找到任意一个会话进行发送
        ByteBuf buf = encode(msg);
        ByteBuf unreleasableBuffer = Unpooled.unreleasableBuffer(buf);
        return receiver
            .concatMap(info -> {
                //  于当前同一个节点
                if (Objects.equals(info.getServerId(), rpcManager.currentServerId())) {
                    return doSendToDevice(msg);
                }
                return rpcManager
                    .getService(info.getServerId(), Service.class)
                    .flatMap(_service -> _service.send(unreleasableBuffer));
            })
            .as(reducer)
            .switchIfEmpty(Reactors.ALWAYS_ZERO)
            .doFinally(ignore -> ReferenceCountUtil.release(buf));
    }

    @Override
    public Mono<Integer> send(String deviceGatewayServerId, Publisher<? extends Message> message) {
        //发给同一个服务节点
        if (StringUtils.hasText(deviceGatewayServerId) && Objects.equals(deviceGatewayServerId, rpcManager.currentServerId())) {
            return Flux
                .from(message)
                .flatMap(this::handleSendToDevice)
                .then(Reactors.ALWAYS_ONE);
        }

        return Flux
            .from(message)
            .cast(DeviceMessage.class)
            .flatMap(msg -> send(deviceGatewayServerId, msg))
            .reduce(0, Integer::sum);
    }

    private Mono<Integer> handleSendToDevice(Message message) {
        return doSendToDevice(message);
    }

    @Override
    public Disposable handleSendToDeviceMessage(String serverId, Function<Message, Mono<Integer>> handler) {
        this.handler.add(handler);
        return () -> this.handler.remove(handler);
    }

    private void addAwaitReplyKey(Message message) {
        if (message instanceof RepayableDeviceMessage<?> msg && !message
            .getHeader(Headers.sendAndForget)
            .orElse(false)) {
            awaits.put(getAwaitReplyKey(msg), msg);
        }
    }

    private Mono<Integer> doSendToDevice(Message message) {
        return TraceHolder
            .writeContextTo(message, Message::addHeader)
            .flatMap(msg -> {
                if (sendToDevice.currentSubscriberCount() == 0 && handler.isEmpty()) {
                    log.warn("no handler for message {}", msg);
                    return doReply(createReply(msg).error(ErrorCode.SYSTEM_ERROR))
                        .then(Reactors.ALWAYS_ZERO);
                }
                if (sendToDevice.currentSubscriberCount() != 0) {
                    try {
                        sendToDevice.emitNext(msg, Reactors.emitFailureHandler());
                    } catch (Throwable err) {
                        return doReply(createReply(msg).error(err))
                            .then(Reactors.ALWAYS_ZERO);
                    }
                }
                return doSendToDevice(msg, handler)
                    .onErrorResume(error -> reply(createReply(message).error(error)).then(Reactors.ALWAYS_ZERO));
            });
    }


    private Mono<Integer> doSendToDevice(Message message, List<Function<Message, Mono<Integer>>> handlers) {
        if (handlers.size() == 1) {
            return handlers
                .get(0)
                .apply(message);
        }
        return Flux
            .fromIterable(handlers)
            .concatMap(h -> h.apply(message))
            .reduce(Math::addExact);
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
        RepayableDeviceMessage<?> request = awaits.remove(getAwaitReplyKey(reply));
        String serviceId = null;

        if (request != null) {
            serviceId = request.getHeader(Headers.sendFrom).orElse(null);
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
        ByteBuf buf = encode(reply);
        ByteBuf unreleasableBuffer = Unpooled.unreleasableBuffer(buf);
        return serviceFlux
            .flatMap(service -> service.reply(unreleasableBuffer))
            .then()
            .doFinally(ignore -> ReferenceCountUtil.release(buf));
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
        } catch (Throwable e) {
            ReferenceCountUtil.safeRelease(buf);
            throw e;
        }
        return buf;
    }

    @io.scalecube.services.annotations.Service
    public interface Service {
        @ServiceMethod
        Mono<Integer> send(ByteBuf payload);

        @ServiceMethod
        Mono<Void> reply(ByteBuf buf);
    }

    private class ServiceImpl implements Service {

        @Override
        public Mono<Integer> send(ByteBuf payload) {
            Message msg = decode(payload);
            addAwaitReplyKey(msg);
            return doSendToDevice(msg);
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
