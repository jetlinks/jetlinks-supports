package org.jetlinks.supports.cluster;

import com.google.common.cache.CacheBuilder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperationBroker;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.device.ReplyFailureHandler;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.exception.DeviceOperationException;
import org.jetlinks.core.message.*;
import org.jetlinks.core.server.MessageHandler;
import org.reactivestreams.Publisher;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.cache.RemovalCause.EXPIRED;

@Slf4j
public abstract class AbstractDeviceOperationBroker implements DeviceOperationBroker, MessageHandler {

    private final Map<String, FluxProcessor<DeviceMessageReply, DeviceMessageReply>> replyProcessor = CacheBuilder
            .newBuilder()
            .expireAfterWrite(Duration.ofMinutes(10))
            .<String, FluxProcessor<DeviceMessageReply, DeviceMessageReply>>removalListener(notify -> {
                if (notify.getCause() == EXPIRED) {
                    try {
                        log.debug("discard await reply message[{}] processor", notify.getKey());
                        notify.getValue().onComplete();
                    } catch (Throwable ignore) {
                    }
                }
            })
            .build()
            .asMap();

    @Override
    public abstract Flux<DeviceStateInfo> getDeviceState(String deviceGatewayServerId, Collection<String> deviceIdList);

    @Override
    public abstract Disposable handleGetDeviceState(String serverId, Function<Publisher<String>, Flux<DeviceStateInfo>> stateMapper);

    @Override
    public Flux<DeviceMessageReply> handleReply(String deviceId, String messageId, Duration timeout) {
        long startWith = System.currentTimeMillis();
        String id = getAwaitReplyKey(deviceId, messageId);
        return replyProcessor
                .computeIfAbsent(id, ignore -> UnicastProcessor.create())
                .timeout(timeout, Mono.error(() -> new DeviceOperationException(ErrorCode.TIME_OUT)))
                .doFinally(signal -> {
                    log.trace("reply device message {} {} take {}ms", deviceId, messageId, System.currentTimeMillis() - startWith);
                    replyProcessor.remove(id);
                    fragmentCounter.remove(id);
                });
    }

    @Override
    public abstract Mono<Integer> send(String deviceGatewayServerId, Publisher<? extends Message> message);

    @Override
    public abstract Mono<Integer> send(Publisher<? extends BroadcastMessage> message);

    @Override
    public abstract Flux<Message> handleSendToDeviceMessage(String serverId);

    protected abstract Mono<Void> doReply(DeviceMessageReply reply);

    private final Map<String, AtomicInteger> fragmentCounter = new ConcurrentHashMap<>();

    protected String getAwaitReplyKey(DeviceMessage message) {
        return getAwaitReplyKey(message.getDeviceId(), message.getMessageId());
    }

    protected String getAwaitReplyKey(String deviceId, String messageId) {
        return deviceId + ":" + messageId;
    }

    @Override
    public Mono<Boolean> reply(DeviceMessageReply message) {
        if (StringUtils.isEmpty(message.getMessageId())) {
            log.warn("reply message messageId is empty: {}", message);
            return Mono.just(false);
        }
        return Mono
                .defer(() -> {
                    String msgId = message.getHeader(Headers.fragmentBodyMessageId).orElse(message.getMessageId());
                    if (message.getHeader(Headers.async).orElse(false)
                            || replyProcessor.containsKey(getAwaitReplyKey(message.getDeviceId(), msgId))) {
                        handleReply(message);
                        return Mono.just(true);
                    }
                    return this
                            .doReply(message)
                            .thenReturn(true);
                });
    }

    protected void handleReply(DeviceMessageReply message) {
        try {
            String messageId = getAwaitReplyKey(message);

            String partMsgId = message.getHeader(Headers.fragmentBodyMessageId).orElse(null);
            if (partMsgId != null) {
                log.trace("handle fragment device[{}] message {}", message.getDeviceId(), message);
                partMsgId = getAwaitReplyKey(message.getDeviceId(), partMsgId);
                FluxProcessor<DeviceMessageReply, DeviceMessageReply> processor = replyProcessor
                        .getOrDefault(partMsgId, replyProcessor.get(messageId));

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
