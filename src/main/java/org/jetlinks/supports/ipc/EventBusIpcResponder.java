package org.jetlinks.supports.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.defaults.DirectCodec;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.ipc.IpcDefinition;
import org.jetlinks.core.ipc.IpcInvoker;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
class EventBusIpcResponder<REQ, RES> implements Disposable {

    private final EventBus eventBus;

    private final IpcDefinition<REQ, RES> definition;

    private final IpcInvoker<REQ, RES> invoker;

    private final Map<Integer, Sinks.Many<REQ>> pendingChannelSinksManyMap = new ConcurrentHashMap<>();

    private final String acceptTopic;

    private Disposable disposable;

    EventBusIpcResponder(EventBus eventBus,
                         IpcDefinition<REQ, RES> definition,
                         IpcInvoker<REQ, RES> invoker) {
        this.eventBus = eventBus;
        this.definition = definition;
        this.invoker = invoker;
        this.acceptTopic = "/_ipc/" + (definition.getAddress().replace("/", "-")) + "/" + invoker.getName();
        init();
    }

    void init() {

        disposable = eventBus
                .subscribe(Subscription
                                   .builder()
                                   .subscriberId(definition.getAddress())
                                   .local()
                                   .broker()
                                   .shared()
                                   .topics(acceptTopic)
                                   .build())
                .flatMap(this::handleRequest)
                .subscribe();
    }

    private Mono<Void> handleRequest(TopicPayload payload) {
        try {

            return this
                    .handleRequest(IpcRequest.decode(payload, definition.requestCodec()))
                    .onErrorResume(err -> {
                        log.error(err.getMessage(), err);
                        return Mono.empty();
                    });

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
        return Mono.empty();
    }

    private Mono<Void> handleRequest(IpcRequest<REQ> request) {
        int consumerId = request.getConsumerId();
        int messageId = request.getMessageId();
        log.trace("handle ipc request {} {}", request.getType(), messageId);
        switch (request.getType()) {
            case fireAndForget:
                return invoker.fireAndForget(request.getRequest());
            case noArgFireAndForget:
                return invoker.fireAndForget();
            case request:
                return this.handleInvoke(consumerId, messageId, invoker.request(request.getRequest()));
            case noArgRequest:
                return this.handleInvoke(consumerId, messageId, invoker.request());
            case requestStream:
                return this.handleInvoke(consumerId, messageId, invoker.requestStream(request.getRequest()));
            case noArgRequestStream:
                return this.handleInvoke(consumerId, messageId, invoker.requestStream());
            case requestChannel:
                pendingChannelSinksManyMap
                        .computeIfAbsent(messageId, ignore -> {
                            Sinks.Many<REQ> processor = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE);
                            this.handleInvoke(consumerId, messageId, invoker.requestChannel(processor.asFlux()))
                                .subscribe();
                            return processor;
                        })
                        .tryEmitNext(request.getRequest());
                return Mono.empty();
            case cancel:
                Optional.ofNullable(pendingChannelSinksManyMap.remove(messageId))
                        .ifPresent(Sinks.Many::tryEmitComplete);
            default:
                return Mono.empty();
        }
    }

    private Mono<Void> handleInvoke(int consumerId, int messageId, Publisher<RES> result) {
        if (result instanceof Mono) {
            return Mono
                    .from(result)
                    .switchIfEmpty(Mono.defer(() -> this
                            .doReply(consumerId, messageId, -1, ResponseType.complete, null)
                            .then(Mono.empty()))
                    )
                    .flatMap(res -> this.doReply(consumerId, messageId, -1, ResponseType.complete, res))
                    .onErrorResume(err -> this.doReply(consumerId, messageId, err));
        }
        AtomicReference<Integer> seqRef = new AtomicReference<>(-1);

        return eventBus
                .publish(acceptTopic + "/" + consumerId + "/_reply", DirectCodec.instance(), Flux
                        .from(result)
                        .index()
                        .map(tp2 -> {
                            int seq = tp2.getT1().intValue();
                            seqRef.set(seq);
                            return Payload
                                    .of(IpcResponse
                                                .of(ResponseType.next, seq, messageId, tp2.getT2(), null)
                                                .toByteBuf(definition.responseCodec(), definition.errorCodec()));
                        })
                )
                .flatMap(i -> this.doReply(consumerId, messageId, seqRef.get(), ResponseType.complete, null))
                .doOnError(err -> {
                    log.warn("reply [{}.{}] error", consumerId, messageId, err);
                    this.doReply(consumerId, messageId, err).subscribe();
                })
                .then();
    }


    private Mono<Void> doReply(int consumerId, int messageId, Throwable throwable) {
        return doReply(consumerId, IpcResponse
                .<RES>of(ResponseType.error, -1, messageId, null, throwable)
                .toByteBuf(definition.responseCodec(), definition.errorCodec()));
    }

    private Mono<Void> doReply(int consumerId, int messageId, int seq, ResponseType responseType, RES response) {
        return this
                .doReply(consumerId, IpcResponse
                        .of(responseType, seq, messageId, response, null)
                        .toByteBuf(definition.responseCodec(), definition.errorCodec()));
    }

    private Mono<Void> doReply(int consumerId, ByteBuf byteBuf) {
        Payload payload = Payload.of(byteBuf);

        return eventBus
                .publish(acceptTopic + "/" + consumerId + "/_reply", payload)
                .doOnNext(i -> {
                    if (i == 0) {
                        log.warn("reply ipc failed,no consumer[{}] listener", consumerId);
                        ReferenceCountUtil.safeRelease(payload);
                    }
                })
                .then();
    }

    @Override
    public boolean isDisposed() {
        return disposable.isDisposed();
    }

    @Override
    public void dispose() {
        disposable.dispose();
    }
}
