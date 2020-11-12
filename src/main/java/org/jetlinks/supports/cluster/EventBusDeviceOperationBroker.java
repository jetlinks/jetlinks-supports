package org.jetlinks.supports.cluster;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.codec.Codec;
import org.jetlinks.core.codec.Codecs;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.message.*;
import org.jetlinks.supports.cluster.redis.DeviceCheckRequest;
import org.jetlinks.supports.cluster.redis.DeviceCheckResponse;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Slf4j
public class EventBusDeviceOperationBroker extends AbstractDeviceOperationBroker implements Disposable {

    private static final Codec<Message> messageCodec = Codecs.lookup(Message.class);

    private final String serverId;

    private final EventBus eventBus;

    private final Disposable.Composite disposable = Disposables.composite();

    private final Map<String, FluxProcessor<DeviceCheckResponse, DeviceCheckResponse>> checkRequests = new ConcurrentHashMap<>();

    private Function<Publisher<String>, Flux<DeviceStateInfo>> localStateChecker;

    private final Map<String, RepayableDeviceMessage<?>> awaits = new ConcurrentHashMap<>();

    public EventBusDeviceOperationBroker(String serverId, EventBus eventBus) {
        this.serverId = serverId;
        this.eventBus = eventBus;
    }

    @Override
    public void dispose() {
        disposable.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposable.isDisposed();
    }

    public void start() {
        {
            Subscription subscription = Subscription
                    .of("device-message-broker",
                        new String[]{"/_sys/msg-broker-reply/" + serverId},
                        Subscription.Feature.broker);

            disposable.add(
                    eventBus
                            .subscribe(subscription, messageCodec)
                            .filter(DeviceMessageReply.class::isInstance)
                            .cast(DeviceMessageReply.class)
                            .subscribe(this::handleReply)
            );
        }

        {
            Subscription subscription = Subscription
                    .of("device-state-checker",
                        new String[]{"/_sys/device-state-check-res/" + serverId},
                        Subscription.Feature.broker);

            disposable.add(eventBus
                                   .subscribe(subscription, DeviceCheckResponse.class)
                                   .subscribe(response -> Optional
                                           .ofNullable(checkRequests.remove(response.getRequestId()))
                                           .ifPresent(processor -> {
                                               processor.onNext(response);
                                               processor.onComplete();
                                           }))
            );
        }

        long awaitTimeout = Duration.ofMinutes(10).toMillis();
        disposable.add(Flux.interval(Duration.ofMinutes(10))
                           .subscribe(ignore -> {

                               awaits.entrySet()
                                     .stream()
                                     .filter(e -> System.currentTimeMillis() - e
                                             .getValue()
                                             .getTimestamp() > awaitTimeout)
                                     .map(Map.Entry::getKey)
                                     .forEach(awaits::remove);

                           }));

    }

    @Override
    public Flux<DeviceStateInfo> getDeviceState(String deviceGatewayServerId,
                                                Collection<String> deviceIdList) {
        return Flux.defer(() -> {
            //本地检查
            if (serverId.equals(deviceGatewayServerId) && localStateChecker != null) {
                return localStateChecker.apply(Flux.fromIterable(deviceIdList));
            }
            long startWith = System.currentTimeMillis();
            String uid = UUID.randomUUID().toString();

            DeviceCheckRequest request = new DeviceCheckRequest(serverId, uid, new ArrayList<>(deviceIdList));
            EmitterProcessor<DeviceCheckResponse> processor = EmitterProcessor.create(true);

            checkRequests.put(uid, processor);

            return eventBus
                    .publish("/_sys/device-state-check/".concat(deviceGatewayServerId), request)
                    .thenMany(processor.flatMap(deviceCheckResponse -> Flux.fromIterable(deviceCheckResponse.getStateInfoList())))
                    .timeout(Duration.ofSeconds(5), Flux.empty())
                    .doFinally((s) -> {
                        log.trace("check device state complete take {}ms", System.currentTimeMillis() - startWith);
                        checkRequests.remove(uid);
                    });
        });
    }

    @Override
    public Disposable handleGetDeviceState(String serverId, Function<Publisher<String>, Flux<DeviceStateInfo>> stateMapper) {
        Subscription subscription = Subscription
                .of("device-state-checker",
                    new String[]{"/_sys/device-state-check/" + serverId},
                    Subscription.Feature.broker);
        this.localStateChecker = stateMapper;
        return eventBus
                .subscribe(subscription, DeviceCheckRequest.class)
                .subscribe(request -> stateMapper
                        .apply(Flux.fromIterable(request.getDeviceId()))
                        .collectList()
                        .map(resp -> new DeviceCheckResponse(resp, request.getRequestId()))
                        .flatMap(res -> eventBus.publish("/_sys/device-state-check-res/" + request.getFrom(), res))
                        .subscribe());
    }


    @Override
    protected Mono<Void> doReply(DeviceMessageReply reply) {
        String serverId = Optional
                .ofNullable(awaits.remove(getAwaitReplyKey(reply)))
                .flatMap(req -> req.getHeader(Headers.sendFrom))
                .orElse("*");

        return eventBus
                .publish("/_sys/msg-broker-reply/" + serverId, messageCodec, reply)
                .then();
    }

    @Override
    public Mono<Integer> send(String deviceGatewayServerId, Publisher<? extends Message> message) {
        return eventBus
                .publish("/_sys/msg-broker/" + deviceGatewayServerId, messageCodec,
                         Flux.from(message)
                             .doOnNext(msg -> msg.addHeader(Headers.sendFrom, serverId))
                )
                .map(Long::intValue);
    }

    @Override
    public Mono<Integer> send(Publisher<? extends BroadcastMessage> message) {
        return eventBus
                .publish("/_sys/msg-broker-broadcast", messageCodec, message)
                .map(Long::intValue);
    }

    @Override
    public Flux<Message> handleSendToDeviceMessage(String serverId) {
        Subscription subscription = Subscription
                .of("device-message-broker",
                    new String[]{"/_sys/msg-broker/" + serverId},
                    Subscription.Feature.local,
                    Subscription.Feature.broker);

        return eventBus
                .subscribe(subscription, messageCodec)
                .doOnNext(message -> {
                    if (message instanceof RepayableDeviceMessage) {
                        boolean isSameServer = message
                                .getHeader(Headers.sendFrom)
                                .map(sendFrom -> sendFrom.equals(serverId))
                                .orElse(false);

                        if (!isSameServer) {
                            awaits.put(getAwaitReplyKey((RepayableDeviceMessage<?>) message), (RepayableDeviceMessage<?>) message);
                        }
                    }
                });
    }

}
