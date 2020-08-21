package org.jetlinks.supports.cluster;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.device.DeviceOperationBroker;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.device.ReplyFailureHandler;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.exception.DeviceOperationException;
import org.jetlinks.core.message.BroadcastMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.supports.cluster.redis.DeviceCheckRequest;
import org.jetlinks.supports.cluster.redis.DeviceCheckResponse;
import org.reactivestreams.Publisher;
import org.springframework.util.StringUtils;
import reactor.core.publisher.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Slf4j
public class ClusterDeviceOperationBroker implements DeviceOperationBroker, MessageHandler {

    private ClusterManager clusterManager;
    private String serverId;


    private Map<String, FluxProcessor<DeviceMessageReply, DeviceMessageReply>> replyProcessor = new ConcurrentHashMap<>();
    private Map<String, FluxProcessor<DeviceCheckResponse, DeviceCheckResponse>> checkRequests = new ConcurrentHashMap<>();


    private Function<Publisher<String>, Flux<DeviceStateInfo>> localStateChecker;

    public ClusterDeviceOperationBroker(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.serverId = clusterManager.getCurrentServerId();
        init();
    }

    public void init() {
        //接收设备状态检查
        clusterManager.<DeviceCheckResponse>getTopic("device:state:check:result:".concat(serverId)).subscribe()
                .subscribe(msg ->
                        Optional.ofNullable(checkRequests.remove(msg.getRequestId()))
                                .ifPresent(processor -> {
                                    processor.onNext(msg);
                                    processor.onComplete();
                                }));
        //接收消息返回
        clusterManager.getTopic("device:msg:reply")
                .subscribe()
                .subscribe(msg -> {
                    if (msg instanceof DeviceMessageReply) {
                        handleReply(((DeviceMessageReply) msg));
                    }
                });
    }

    @Override
    public Flux<DeviceStateInfo> getDeviceState(String deviceGatewayServerId, Collection<String> deviceIdList) {
        return Flux.defer(() -> {
            //本地检查
            if (serverId.equals(deviceGatewayServerId) && localStateChecker != null) {
                return localStateChecker.apply(Flux.fromIterable(deviceIdList));
            }
            String uid = UUID.randomUUID().toString();

            DeviceCheckRequest request = new DeviceCheckRequest(serverId, uid, new ArrayList<>(deviceIdList));
            EmitterProcessor<DeviceCheckResponse> processor = EmitterProcessor.create(true);

            checkRequests.put(uid, processor);

            return clusterManager.getTopic("device:state:checker:".concat(deviceGatewayServerId))
                    .publish(Mono.just(request))
                    .flatMapMany(m -> processor.flatMap(deviceCheckResponse -> Flux.fromIterable(deviceCheckResponse.getStateInfoList())))
                    .timeout(Duration.ofSeconds(5), Flux.empty());
        });
    }

    @Override
    public void handleGetDeviceState(String serverId, Function<Publisher<String>, Flux<DeviceStateInfo>> stateMapper) {
        localStateChecker = stateMapper;
        clusterManager.<DeviceCheckRequest>getTopic("device:state:checker:".concat(serverId))
                .subscribe()
                .subscribe(request ->
                        stateMapper.apply(Flux.fromIterable(request.getDeviceId()))
                                .collectList()
                                .map(resp -> new DeviceCheckResponse(resp, request.getRequestId()))
                                .as(clusterManager.getTopic("device:state:check:result:".concat(request.getFrom()))::publish)
                                .subscribe(len -> {
                                    if (len <= 0) {
                                        log.warn("device check reply fail");
                                    }
                                }));
    }

    @Override
    public Flux<DeviceMessageReply> handleReply(String messageId, Duration timeout) {
        return replyProcessor
                .computeIfAbsent(messageId, ignore -> UnicastProcessor.create())
                .timeout(timeout, Mono.error(() -> new DeviceOperationException(ErrorCode.TIME_OUT)))
                .doFinally(signal -> {
                    replyProcessor.remove(messageId);
                    fragmentCounter.remove(messageId);
                });
    }

    @Override
    public Mono<Integer> send(String deviceGatewayServerId, Publisher<? extends Message> message) {

        return Flux.from(message)
                .map(msg -> msg.addHeader(Headers.sendFrom, clusterManager.getCurrentServerId()))
                .flatMap(msg -> clusterManager.getTopic("device:msg:p2p:".concat(deviceGatewayServerId)).publish(Mono.just(msg)))
                .takeWhile(l -> l > 0)
                .last(0);
    }

    @Override
    public Mono<Integer> send(Publisher<? extends BroadcastMessage> message) {

        return clusterManager
                .<BroadcastMessage>getTopic("device:msg:broadcast")
                .publish(message);
    }

    @Override
    public Flux<Message> handleSendToDeviceMessage(String serverId) {
        return clusterManager
                .getTopic("device:msg:p2p:".concat(serverId))
                .subscribe()
                .cast(Message.class);
    }

    private final Map<String, AtomicInteger> fragmentCounter = new ConcurrentHashMap<>();

    @Override
    public Mono<Boolean> reply(DeviceMessageReply message) {
        if (StringUtils.isEmpty(message.getMessageId())) {
            log.warn("reply message messageId is empty: {}", message);
            return Mono.just(false);
        }
        return Mono.defer(() -> {
            message.addHeader(Headers.replyFrom, serverId);
            if (replyProcessor.containsKey(message.getMessageId())) {
                handleReply(message);
                return Mono.just(true);
            }
            return clusterManager
                    .getTopic("device:msg:reply")
                    .publish(Mono.just(message))
                    .map(l -> l > 0)
                    .switchIfEmpty(Mono.just(false));
        });
    }

    private void handleReply(DeviceMessageReply message) {
        try {
            String messageId = message.getMessageId();
            if (StringUtils.isEmpty(messageId)) {
                log.warn("reply message messageId is empty: {}", message);
                return;
            }

            String partMsgId = message.getHeader(Headers.fragmentBodyMessageId).orElse(null);
            if (partMsgId != null) {
                FluxProcessor<DeviceMessageReply, DeviceMessageReply> processor = replyProcessor.getOrDefault(partMsgId, replyProcessor.get(messageId));

                if (processor == null || processor.isDisposed()) {
                    replyProcessor.remove(partMsgId);
                    return;
                }
                int partTotal = message.getHeader(Headers.fragmentNumber).orElse(1);
                AtomicInteger counter = fragmentCounter.computeIfAbsent(partMsgId, r -> new AtomicInteger(partTotal));

                try {
                    processor.onNext(message);
                } finally {
                    if (counter.decrementAndGet() <= 0 || message.getHeader(Headers.fragmentLast).orElse(false)) {
                        try {
                            processor.onComplete();
                        } finally {
                            replyProcessor.remove(partMsgId);
                            fragmentCounter.remove(partMsgId);
                        }
                    }
                }
                return;
            }
            FluxProcessor<DeviceMessageReply, DeviceMessageReply> processor = replyProcessor.get(messageId);

            if (processor != null && !processor.isDisposed()) {
                processor.onNext(message);
                processor.onComplete();
            } else {
                replyProcessor.remove(messageId);
            }
        } catch (Exception e) {
            replyFailureHandler.handle(e, message);
        }
    }

    @Setter
    private ReplyFailureHandler replyFailureHandler = (error, message) -> log.warn("unhandled reply message:{}", message, error);


}
