package org.jetlinks.supports.rpc;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.NativePayload;
import org.jetlinks.core.Payload;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.rpc.Invoker;
import org.jetlinks.core.rpc.RpcDefinition;
import org.jetlinks.core.rpc.RpcService;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

@AllArgsConstructor
@Slf4j
public class EventBusRcpService implements RpcService {

    private final EventBus eventBus;

    @Override
    public <REQ, RES> Disposable listen(RpcDefinition<REQ, RES> definition, BiFunction<String, REQ, Publisher<RES>> call) {

        return doListen(definition, (s, reqPublisher) -> Flux.from(reqPublisher).flatMap(req -> call.apply(s, req)));
    }

    @Override
    public <RES> Disposable listen(RpcDefinition<Void, RES> definition, Function<String, Publisher<RES>> call) {
        return doListen(definition, (topic, request) -> Flux.from(request).thenMany(call.apply(topic)));
    }

    @Override
    public <REQ, RES> Invoker<REQ, RES> createInvoker(RpcDefinition<REQ, RES> definition) {
        String reqTopic = definition.getAddress();
        String reqTopicRes = definition.getAddress() + "/_reply";
        AtomicLong idInc = new AtomicLong();
        Map<Long, FluxSink<RcpResult>> request = new ConcurrentHashMap<>();
        Disposable disposable = eventBus
                .subscribe(
                        Subscription.of(
                                definition.getId(),
                                reqTopicRes,
                                Subscription.Feature.local,
                                Subscription.Feature.broker))
                .doOnNext(payload -> {
                    RcpResult result = RcpResult.parse(payload);
                    FluxSink<RcpResult> sink = request.get(result.getRequestId());
                    if (null != sink) {
                        sink.next(result);
                    }
                })
                .onErrorContinue((err, obj) -> {
                    log.error(err.getMessage(), err);
                })
                .subscribe();


        return new Invoker<REQ, RES>() {

            @Override
            public Flux<RES> invoke() {
                return invoke((Publisher<? extends REQ>) null);
            }

            private Mono<Long> doSend(long id, Publisher<? extends REQ> payload) {
                if (payload instanceof Mono) {
                    return Mono.from(payload)
                            .flatMap(req -> eventBus.publish(reqTopic, RpcRequest.nextAndComplete(id, definition.requestCodec().encode(req))))
                            ;
                } else if (payload instanceof Flux) {
                    return Flux.from(payload)
                            .map(req -> RpcRequest.next(id, definition.requestCodec().encode(req)))
                            .as(req -> eventBus.publish(reqTopic, req))
                            .doOnSuccess((v) -> eventBus.publish(reqTopic, RpcRequest.complete(id)).subscribe())
                            ;
                } else {
                    return eventBus.publish(reqTopic, RpcRequest.nextAndComplete(id, Payload.voidPayload));
                }
            }

            @Override
            public Flux<RES> invoke(Publisher<? extends REQ> payload) {
                return Flux.<RcpResult>create(sink -> {
                    long id = idInc.incrementAndGet();
                    request.put(id, sink);
                    sink.onDispose(() -> request.remove(id));
                    log.trace("do invoke rpc:{}", definition.getAddress());
                    doSend(id, payload)
                            .doOnNext(l -> {
                                if (l == 0) {
                                    sink.error(new UnsupportedOperationException("no rpc service for:" + definition.getAddress()));
                                }
                            })
                            .doOnError(sink::error)
                            .subscribe();

                }).<RES>handle((res, sink) -> {
                    if (res.getType() == RcpResult.Type.RESULT_AND_COMPLETE) {
                        RES r = definition.responseCodec().decode(res);
                        if (r != null) {
                            sink.next(r);
                        }
                        sink.complete();
                    } else if (res.getType() == RcpResult.Type.RESULT) {
                        RES r = definition.responseCodec().decode(res);
                        if (r != null) {
                            sink.next(r);
                        }
                    } else if (res.getType() == RcpResult.Type.COMPLETE) {
                        sink.complete();
                    } else if (res.getType() == RcpResult.Type.ERROR) {
                        Throwable e = definition.errorCodec().decode(res);
                        if (e != null) {
                            sink.error(e);
                        } else {
                            sink.complete();
                        }
                    }
                }).timeout(Duration.ofSeconds(10));
            }

            @Override
            public void dispose() {
                disposable.dispose();
            }
        };
    }

    protected Mono<Void> reply(String topic, RcpResult result) {
        return eventBus
                .publish(topic, result)
                .then();
    }

    private class PendingRequest<REQ, RES> {
        long requestId;
        String reqTopicRes;
        String reqTopic;
        RpcDefinition<REQ, RES> definition;
        BiFunction<String, Publisher<REQ>, Publisher<RES>> invoker;
        Disposable disposable;
        EmitterProcessor<REQ> processor = EmitterProcessor.create();
        FluxSink<REQ> sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);

        public PendingRequest(long requestId,
                              RpcDefinition<REQ, RES> definition,
                              BiFunction<String, Publisher<REQ>, Publisher<RES>> invoker,
                              Disposable disposable) {
            this.requestId = requestId;
            this.reqTopic = definition.getAddress();
            this.reqTopicRes = definition.getAddress() + "/_reply";
            this.definition = definition;
            this.invoker = invoker;
            this.disposable = disposable;
            Flux.from(invoker.apply(reqTopic, processor))
                    .flatMap(res -> reply(reqTopicRes, RcpResult.result(requestId, NativePayload.of(res, definition.responseCodec()::encode))))
                    .doOnComplete(() -> reply(reqTopicRes, RcpResult.complete(requestId)).subscribe())
                    .doOnError((e) -> {
                        log.error(e.getMessage(), e);
                        reply(reqTopicRes, RcpResult.error(requestId, NativePayload.of(e, definition.errorCodec()::encode))).subscribe();
                    })
                    .subscribe();
            sink.onDispose(disposable);
        }

        void next(RpcRequest req) {
            try {
                if (req.getType() == RpcRequest.Type.COMPLETE) {
                    sink.complete();
                    return;
                }
                REQ v = definition.requestCodec().decode(req);
                if (v != null) {
                    sink.next(v);
                }
                if (req.getType() == RpcRequest.Type.NEXT_AND_END) {
                    sink.complete();
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
                sink.error(e);
            }
        }

    }

    private <REQ, RES> Disposable doListen(RpcDefinition<REQ, RES> definition,
                                           BiFunction<String, Publisher<REQ>, Publisher<RES>> invokeResult) {

        Map<Long, PendingRequest<REQ, RES>> request = new ConcurrentHashMap<>();

        //订阅请求
        return eventBus
                .subscribe(Subscription.of(definition.getId(),
                        definition.getAddress(),
                        Subscription.Feature.local,
                        Subscription.Feature.broker))
                .map(RpcRequest::parse)
                .doOnCancel(request::clear)
                .subscribe(_req -> request.computeIfAbsent(_req.getRequestId(),
                        id -> new PendingRequest<>(id, definition, invokeResult, () -> request.remove(id)))
                        .next(_req)
                );
    }

}
