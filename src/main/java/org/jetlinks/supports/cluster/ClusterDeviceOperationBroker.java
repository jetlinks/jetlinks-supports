package org.jetlinks.supports.cluster;

import com.google.common.cache.CacheBuilder;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.*;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.cache.RemovalCause.EXPIRED;

@Deprecated
@Slf4j
public class ClusterDeviceOperationBroker extends AbstractDeviceOperationBroker {

    private static final String QUALIFIER_REPLY = "cdob_r";
    private static final String QUALIFIER_SEND = "cdob_s";

    final ExtendedCluster cluster;

    final DeviceSessionManager sessionManager;

    private final Map<String, RepayableDeviceMessage<?>> awaits = CacheBuilder
            .newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .<String, RepayableDeviceMessage<?>>removalListener(notify -> {
                if (notify.getCause() == EXPIRED) {
                    try {
                        ClusterDeviceOperationBroker.log.debug("discard await reply message[{}] message,{}", notify.getKey(), notify.getValue());
                    } catch (Throwable ignore) {
                    }
                }
            })
            .build()
            .asMap();

    private final Sinks.Many<Message> sendToDevice = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE,false);

    public ClusterDeviceOperationBroker(ExtendedCluster cluster,
                                        DeviceSessionManager sessionManager) {
        this.cluster = cluster;
        this.sessionManager = sessionManager;
        cluster.handler(ignore -> new ClusterHandler());
    }

    class ClusterHandler implements ClusterMessageHandler {
        @Override
        public void onGossip(io.scalecube.cluster.transport.api.Message gossip) {
            onMessage(gossip);
        }

        @Override
        public void onMessage(io.scalecube.cluster.transport.api.Message message) {
            if (QUALIFIER_SEND.equals(message.qualifier())) {
                Message msg = message.data();
                TraceHolder.copyContext(message.headers(), msg, Message::addHeader);
                handleSendToDevice(message.data())
                        .contextWrite(TraceHolder.readToContext(Context.empty(), message.headers()))
                        .subscribe();
            } else if (QUALIFIER_REPLY.equals(message.qualifier())) {
                Message msg = message.data();
                TraceHolder.copyContext(message.headers(), msg, Message::addHeader);
                handleReply(message.data());
            }
        }
    }

    private String currentServerId() {
        return sessionManager.getCurrentServerId();
    }

    @Override
    public Flux<DeviceStateInfo> getDeviceState(String deviceGatewayServerId, Collection<String> deviceIdList) {
        return Flux
                .fromIterable(deviceIdList)
                .flatMap(id -> sessionManager
                        .checkAlive(id, false)
                        .map(alive -> new DeviceStateInfo(id, alive ? DeviceState.online : DeviceState.offline))
                );
    }

    @Override
    public Flux<DeviceMessageReply> handleReply(String deviceId, String messageId, Duration timeout) {
        return super
                .handleReply(deviceId, messageId, timeout)
                .doOnCancel(() -> {
                    awaits.remove(getAwaitReplyKey(deviceId, messageId));
                });
    }

    @Override
    public Mono<Integer> send(String deviceGatewayServerId,
                              Publisher<? extends Message> message) {
        //发给同一个服务节点
        if (currentServerId().equals(deviceGatewayServerId)) {
            return Flux
                    .from(message)
                    .flatMap(this::handleSendToDevice)
                    .then(Reactors.ALWAYS_ONE);
        }

        Member member = getMember(deviceGatewayServerId);
        if (null == member) {
            return Reactors.ALWAYS_ZERO;
        }

        return Flux
                .from(message)
                .flatMap(msg -> {
                    msg.addHeader(Headers.sendFrom, sessionManager.getCurrentServerId());
                    addAwaitReplyKey(msg);
                    return cluster
                            .send(member, io.scalecube.cluster.transport.api.Message
                                    .builder()
                                    .qualifier(QUALIFIER_SEND)
                                    .data(msg)
                                    .build());
                })
                .then(Reactors.ALWAYS_ONE);
    }

    @Override
    public Mono<Integer> send(Publisher<? extends BroadcastMessage> message) {
        return Reactors.ALWAYS_ZERO;
    }

    @Override
    public Flux<Message> handleSendToDeviceMessage(String serverId) {
        return sendToDevice.asFlux();
    }

    private Mono<Void> handleSendToDevice(Message message) {  
        addAwaitReplyKey(message);
        if (sendToDevice.currentSubscriberCount() == 0) {
            log.warn("no handler for message {}", message);
            return doReply(createReply(message).error(ErrorCode.SYSTEM_ERROR));
        }
        try {
            sendToDevice.emitNext(message, Reactors.emitFailureHandler());
        }catch (Throwable err){
            return doReply(createReply(message).error(err));
        }
        return Mono.empty();
    }
    
   private void addAwaitReplyKey(Message message){
       if (message instanceof RepayableDeviceMessage && !message
                .getHeader(Headers.sendAndForget)
                .orElse(false)) {  
           RepayableDeviceMessage<?> msg = ((RepayableDeviceMessage<?>) message);      
           awaits.put(getAwaitReplyKey(msg), msg);
        }
    }

    private DeviceMessageReply createReply(Message message) {
        if (message instanceof RepayableDeviceMessage) {
            return ((RepayableDeviceMessage<?>) message).newReply();
        }
        return new CommonDeviceMessageReply<>()
                .messageId(message.getMessageId());
    }


    @Override
    protected Mono<Void> doReply(DeviceMessageReply reply) {
        RepayableDeviceMessage<?> msg = awaits.remove(getAwaitReplyKey(reply));
        Member member = null;

        if (null != msg) {
            member = msg.getHeader(Headers.sendFrom)
                        .map(this::getMember)
                        .orElse(null);
        }

        Function<Member, Mono<Void>> handler = _member -> cluster
                .send(_member, io.scalecube.cluster.transport.api.Message
                        .builder()
                        .qualifier(QUALIFIER_REPLY)
                        .data(reply)
                        .build());
        //fast reply
        if (null != member) {
            return handler.apply(member);
        }
        return Flux
                .fromIterable(cluster.otherMembers())
                .flatMap(handler)
                .then();
    }

    @Override
    public Disposable handleGetDeviceState(String serverId, Function<Publisher<String>, Flux<DeviceStateInfo>> stateMapper) {
        return Disposables.disposed();
    }

    public Member getMember(String id) {
        for (Member member : cluster.otherMembers()) {
            if (Objects.equals(member.id(), id) || Objects.equals(member.alias(), id)) {
                return member;
            }
        }
        return null;
    }

}
