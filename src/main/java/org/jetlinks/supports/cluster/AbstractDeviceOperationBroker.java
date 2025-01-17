package org.jetlinks.supports.cluster;

import com.google.common.cache.CacheBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperationBroker;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.device.ReplyFailureHandler;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.exception.DeviceOperationException;
import org.jetlinks.core.message.*;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.utils.Reactors;
import org.reactivestreams.Publisher;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public abstract class AbstractDeviceOperationBroker implements DeviceOperationBroker, MessageHandler {

    protected final Map<AwaitKey, Awaiting> replyProcessor = new ConcurrentHashMap<>();

    private final Map<AwaitKey, AtomicInteger> fragmentCounter = CacheBuilder
        .newBuilder()
        .expireAfterWrite(Duration.ofMinutes(5))
        .<AwaitKey, AtomicInteger>build()
        .asMap();

    private final NavigableMap<PendingKey, String> pendingNoMessageId = new ConcurrentSkipListMap<>();

    @Override
    public abstract Flux<DeviceStateInfo> getDeviceState(String deviceGatewayServerId, Collection<String> deviceIdList);

    @Override
    public abstract Disposable handleGetDeviceState(String serverId, Function<Publisher<String>, Flux<DeviceStateInfo>> stateMapper);

    @Override
    public Flux<DeviceMessageReply> handleReply(DeviceMessage message, Duration timeout) {
        //标记了设备不会回复messageId
        if (message instanceof RepayableDeviceMessage
            && message.getHeaderOrDefault(Headers.replyNoMessageId)) {
            MessageType replyType = ((RepayableDeviceMessage<?>) message).getReplyType();
            String messageId = message.getMessageId();
            PendingKey key = new PendingKey(message.getDeviceId(), message.getTimestamp(), replyType);
            pendingNoMessageId.put(key, message.getMessageId());
            return handleReply0(
                message.getDeviceId(),
                message.getMessageId(),
                timeout,
                () -> pendingNoMessageId.remove(key, messageId));
        }

        return handleReply(message.getDeviceId(), message.getMessageId(), timeout);
    }

    protected void checkExpires() {
        //检查已经过期的等待回复
        replyProcessor
            .values()
            .forEach(Awaiting::checkExpires);
    }

    public Flux<DeviceMessageReply> handleReply0(String deviceId, String messageId, Duration timeout, Runnable after) {
        long startWith = System.currentTimeMillis();
        AwaitKey key = getAwaitReplyKey(deviceId, messageId);
        Awaiting awaiting;
        for (; ; ) {
            awaiting = new Awaiting(startWith, key, after);
            if (replyProcessor.put(key, awaiting) == null) {
                break;
            }
            awaiting = replyProcessor.get(key);
            if (awaiting != null) {
                break;
            }
        }
        Flux<DeviceMessageReply> reply = awaiting.asFlux();
        if (!timeout.isZero() && !timeout.isNegative()) {
            reply = reply
                .timeout(timeout, Mono.error(() -> new DeviceOperationException.NoStackTrace(ErrorCode.TIME_OUT)));
        }
        return reply.doFinally(awaiting);
    }

    @Override
    public Flux<DeviceMessageReply> handleReply(String deviceId, String messageId, Duration timeout) {
        return handleReply0(deviceId, messageId, timeout, () -> {
        });
    }

    @Override
    public abstract Mono<Integer> send(String deviceGatewayServerId, Publisher<? extends Message> message);

    @Override
    public abstract Mono<Integer> send(Publisher<? extends BroadcastMessage> message);

    @Override
    public abstract Flux<Message> handleSendToDeviceMessage(String serverId);

    protected abstract Mono<Void> doReply(DeviceMessageReply reply);

    protected AwaitKey getAwaitReplyKey(DeviceMessage message) {
        return getAwaitReplyKey(message.getDeviceId(), message.getMessageId());
    }

    protected AwaitKey getAwaitReplyKey(String deviceId, String messageId) {
        return new AwaitKey(deviceId, messageId);
    }

    protected boolean handleNoMessageIdReply(DeviceMessageReply message) {
        PendingKey from = new PendingKey(message.getDeviceId(),
                                         0,
                                         message.getMessageType());
        PendingKey to = new PendingKey(message.getDeviceId(),
                                       message.getTimestamp(),
                                       message.getMessageType());
        //查找最近的
        Map.Entry<PendingKey, String> entry = pendingNoMessageId
            .subMap(from, false, to, true)
            .firstEntry();

        if (entry != null
            && Objects.equals(entry.getKey().deviceId, message.getDeviceId())
            && Objects.equals(entry.getKey().messageType, message.getMessageType())) {
            message.messageId(entry.getValue());
            pendingNoMessageId.remove(entry.getKey(), entry.getValue());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Mono<Boolean> reply(DeviceMessageReply message) {
        if (!StringUtils.hasText(message.getMessageId())
            && !handleNoMessageIdReply(message)) {
            log.warn("reply message messageId is empty: {}", message);
            return Reactors.ALWAYS_FALSE;
        }

        Mono<Boolean> then = Reactors.ALWAYS_TRUE;
        if (message instanceof ChildDeviceMessageReply) {
            Message childDeviceMessage = ((ChildDeviceMessageReply) message).getChildDeviceMessage();
            if (childDeviceMessage instanceof DeviceMessageReply) {
                then = reply(((DeviceMessageReply) childDeviceMessage));
            }
        }
        return Mono
            .defer(() -> {
                String msgId = message.getHeader(Headers.fragmentBodyMessageId).orElse(message.getMessageId());
                if (message.getHeader(Headers.async).orElse(false)
                    || replyProcessor.containsKey(getAwaitReplyKey(message.getDeviceId(), msgId))) {
                    handleReply(message);
                    return Reactors.ALWAYS_TRUE;
                }
                return this
                    .doReply(message)
                    .thenReturn(true);
            })
            .then(then);
    }

    protected void handleReply(DeviceMessageReply message) {
        try {
            AwaitKey key = getAwaitReplyKey(message);
            String partMsgId = message.getHeader(Headers.fragmentBodyMessageId).orElse(null);
            if (partMsgId != null) {
                log.trace("handle fragment device[{}] message {}", message.getDeviceId(), message);
                AwaitKey _partMsgId = getAwaitReplyKey(message.getDeviceId(), partMsgId);
                Awaiting processor = replyProcessor
                    .getOrDefault(_partMsgId, replyProcessor.get(key));

                if (processor == null || processor.currentSubscriberCount() == 0) {
                    replyProcessor.remove(_partMsgId);
                    return;
                }
                int partTotal = message.getHeader(Headers.fragmentNumber).orElse(1);
                AtomicInteger counter = fragmentCounter.computeIfAbsent(_partMsgId, r -> new AtomicInteger(partTotal));

                try {
                    processor.emitNext(message);
                } finally {
                    if (counter.decrementAndGet() <= 0 || message.getHeader(Headers.fragmentLast).orElse(false)) {
                        try {
                            processor.tryComplete();
                        } finally {
                            replyProcessor.remove(_partMsgId);
                            fragmentCounter.remove(_partMsgId);
                        }
                    }
                }
                return;
            }

            Awaiting processor = replyProcessor.get(key);
            if (processor != null) {
                processor.emitNext(message);
                processor.complete();
            }
        } catch (Throwable e) {
            replyFailureHandler.handle(e, message);
        }
    }

    @Setter
    private ReplyFailureHandler replyFailureHandler =
        (error, message) -> AbstractDeviceOperationBroker.log.info("unhandled reply message:{}", message, error);


    @AllArgsConstructor
    protected class Awaiting implements Consumer<SignalType> {
        long timestamp;
        AwaitKey key;
        Runnable callback;
        final Sinks.Many<DeviceMessageReply> processor
            = Sinks.many()
                   .multicast()
                   .onBackpressureBuffer(Queues.XS_BUFFER_SIZE);

        boolean isExpires() {
            // 10秒没有人订阅? 说明 reply 没有人处理,可能是协议包自定义了拦截器,忽略了reply.
            return currentSubscriberCount() == 0
                && System.currentTimeMillis() - timestamp > 10_000;
        }

        Flux<DeviceMessageReply> asFlux() {
            return processor.asFlux();
        }

        int currentSubscriberCount() {
            return processor.currentSubscriberCount();
        }

        void emitNext(DeviceMessageReply message) {
            processor.emitNext(message, Reactors.emitFailureHandler());
        }

        void tryComplete() {
            processor.tryEmitComplete();
        }

        void complete() {
            processor.emitComplete(Reactors.emitFailureHandler());
        }

        void checkExpires() {
            if (isExpires()) {
                log.info("awaiting device {} message {} reply expires", key.deviceId, key.messageId);
                doFinally();
            }
        }

        void doFinally() {
            if (null != callback) {
                callback.run();
            }
            replyProcessor.remove(key, this);
            fragmentCounter.remove(key);
        }

        @Override
        public void accept(SignalType signalType) {
            if (AbstractDeviceOperationBroker.log.isTraceEnabled()) {
                AbstractDeviceOperationBroker.log
                    .trace("device message {} {} take {}ms",
                           key.deviceId,
                           key.messageId,
                           System.currentTimeMillis() - timestamp);
            }
            doFinally();
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode(cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
    protected static class AwaitKey {
        private String deviceId;
        private String messageId;
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString
    protected static class PendingKey implements Comparable<PendingKey> {
        static Comparator<PendingKey> comparator = Comparator
            .comparing(PendingKey::getDeviceId)
            .thenComparing(PendingKey::getMessageType)
            .thenComparing(PendingKey::getTimestamp);

        private String deviceId;
        private long timestamp;
        private MessageType messageType;

        @Override
        public int compareTo(@Nonnull PendingKey o) {
            return comparator.compare(this, o);
        }
    }
}
