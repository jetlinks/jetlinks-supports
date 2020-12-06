package org.jetlinks.supports.rpc;

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.defaults.DirectCodec;
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
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

@AllArgsConstructor
@Slf4j
public class EventBusRpcService implements RpcService {

    private final EventBus eventBus;

    private final long requesterId = IDGenerator.SNOW_FLAKE.generate();

    @Override
    public <REQ, RES> Disposable listen(RpcDefinition<REQ, RES> definition, BiFunction<String, REQ, Publisher<RES>> call) {

        return doListen(definition, (s, reqPublisher) -> Flux.from(reqPublisher).flatMap(req -> call.apply(s, req)));
    }

    @Override
    public <RES> Disposable listen(RpcDefinition<Void, RES> definition, Function<String, Publisher<RES>> call) {
        return doListen(definition, (topic, request) -> Flux.from(request).thenMany(call.apply(topic)));
    }

    private String getTopic(RpcDefinition<?, ?> definition) {
        String address = definition.getAddress();
        if (!address.startsWith("/")) {
            address = "/" + address;
        }
        if (address.endsWith("/")) {
            address = address.substring(0, address.length() - 1);
        }
        return address;
    }

    @Override
    public <REQ, RES> Invoker<REQ, RES> createInvoker(RpcDefinition<REQ, RES> definition) {
        String reqTopic = getTopic(definition);

        String reqTopicRes = reqTopic + "/" + requesterId + "/_reply";
        Map<Long, FluxSink<RpcResult>> request = new ConcurrentHashMap<>();
        Disposable disposable = eventBus
                .subscribe(
                        Subscription.of(
                                definition.getId(),
                                reqTopicRes,
                                Subscription.Feature.local,
                                Subscription.Feature.broker))
                .doOnNext(payload -> {
                    try {
                        RpcResult result = RpcResult.parse(payload);
                        log.trace("handle rpc[{}] reply {} {}", definition, result.getType(), result.getRequestId());
                        FluxSink<RpcResult> sink = request.get(result.getRequestId());
                        if (null != sink && !sink.isCancelled()) {
                            sink.next(result);
                        }else {
                            log.info("discard rpc[{}] reply {} {}", definition, result.getType(), result.getRequestId());
                        }
                    } finally {
                        ReferenceCountUtil.safeRelease(payload);
                    }
                })
                .onErrorContinue((err, obj) -> log.error(err.getMessage(), err))
                .subscribe();


        return new Invoker<REQ, RES>() {

            @Override
            public Flux<RES> invoke() {
                return invoke((Publisher<? extends REQ>) null);
            }

            private Mono<Long> doSend(long id, Publisher<? extends REQ> payload) {
                if (payload instanceof Mono) {
                    return Mono.from(payload)
                               .flatMap(req -> eventBus
                                       .publish(
                                               reqTopic,
                                               DirectCodec.INSTANCE,
                                               RpcRequest.nextAndComplete(requesterId, id, definition
                                                       .requestCodec()
                                                       .encode(req)),
                                               Schedulers.immediate()
                                       )
                               )
                            ;
                } else if (payload instanceof Flux) {
                    return Flux.from(payload)
                               .map(req -> RpcRequest.next(requesterId, id, definition.requestCodec().encode(req)))
                               .as(req -> eventBus.publish(reqTopic, DirectCodec.INSTANCE, req, Schedulers.immediate()))
                               .doOnSuccess((v) -> eventBus
                                       .publish(reqTopic, RpcRequest.complete(requesterId, id), Schedulers.immediate())
                                       .subscribe())
                            ;
                } else {
                    return eventBus.publish(reqTopic,
                                            DirectCodec.INSTANCE,
                                            RpcRequest.nextAndComplete(requesterId, id, Payload.voidPayload),
                                            Schedulers.immediate());
                }
            }

            @Override
            public Flux<RES> invoke(Publisher<? extends REQ> payload) {
                return Flux
                        .<RpcResult>create(sink -> {
                            long id = IDGenerator.SNOW_FLAKE.generate();
                            request.put(id, sink);
                            sink.onDispose(() -> request.remove(id));
                            log.trace("do invoke rpc:{},requestId:{}", definition.getAddress(), id);
                            this.doSend(id, payload)
                                .doOnNext(l -> {
                                    if (l == 0) {
                                        sink.error(new UnsupportedOperationException("no rpc service for:" + definition
                                                .getAddress()));
                                    }
                                })
                                .doOnError(sink::error)
                                .subscribe();

                        }).<RES>handle((res, sink) -> {
                            try {
                                if (res.getType() == RpcResult.Type.RESULT_AND_COMPLETE) {
                                    RES r = definition.responseCodec().decode(res);
                                    if (r != null) {
                                        sink.next(r);
                                    }
                                    sink.complete();
                                } else if (res.getType() == RpcResult.Type.RESULT) {
                                    RES r = definition.responseCodec().decode(res);
                                    if (r != null) {
                                        sink.next(r);
                                    }
                                } else if (res.getType() == RpcResult.Type.COMPLETE) {
                                    sink.complete();
                                } else if (res.getType() == RpcResult.Type.ERROR) {
                                    Throwable e = definition.errorCodec().decode(res);
                                    if (e != null) {
                                        sink.error(e);
                                    } else {
                                        sink.complete();
                                    }
                                }
                            } finally {
                                ReferenceCountUtil.safeRelease(res);
                            }
                        }).timeout(Duration.ofSeconds(10), Mono.error(() -> new TimeoutException("invoke " + definition + "timeout")));
            }

            @Override
            public void dispose() {
                disposable.dispose();
            }

            @Override
            public boolean isDisposed() {
                return disposable.isDisposed();
            }
        };
    }

    protected Mono<Void> reply(String topic, RpcResult result) {
        return eventBus
                .publish(topic, result, Schedulers.immediate())
                .doOnNext(i -> {
                    if(i==0){
                        log.warn("reply rpc request {} requestId:{} failed: no listener[{}]", result.getType(), result.getRequestId(),topic);
                        return;
                    }
                    log.trace("reply rpc request {} requestId:{}", result.getType(), result.getRequestId());
                })
                .then();
    }

    private class PendingRequest<REQ, RES> {
        long requestId;
        long requesterId;
        String reqTopicRes;
        String reqTopic;
        RpcDefinition<REQ, RES> definition;
        BiFunction<String, Publisher<REQ>, Publisher<RES>> invoker;
        EmitterProcessor<REQ> processor = EmitterProcessor.create(Integer.MAX_VALUE);
        FluxSink<REQ> sink = processor.sink();
        boolean started = false;

        public PendingRequest(long requesterId,
                              long requestId,
                              RpcDefinition<REQ, RES> definition,
                              BiFunction<String, Publisher<REQ>, Publisher<RES>> invoker,
                              Disposable disposable) {
            this.requestId = requestId;
            this.requesterId = requesterId;
            this.reqTopic = getTopic(definition);
            this.reqTopicRes = reqTopic + "/" + requesterId + "/_reply";
            this.definition = definition;
            this.invoker = invoker;
            doStart();
            sink.onDispose(disposable);
        }

        void doStart() {
            if (started) {
                return;
            }
            log.trace("handle rpc request {},requestId:{}", definition, requestId);
            started = true;
            Flux.from(invoker.apply(reqTopic, processor))
                .flatMap(res -> reply(reqTopicRes, RpcResult.result(requestId, definition.responseCodec().encode(res))))
                .doOnComplete(() -> reply(reqTopicRes, RpcResult.complete(requestId)).subscribe())
                .doOnError((e) -> {
                    log.error(e.getMessage(), e);
                    reply(reqTopicRes, RpcResult.error(requestId, definition.errorCodec().encode(e)))
                            .subscribe();
                })
                .subscribe();
        }

        void release(){
            processor.onComplete();
        }
        void next(RpcRequest req) {
            try {
                if (req.getType() == RpcRequest.Type.COMPLETE) {
                    sink.complete();
                    return;
                }
                REQ v = req.decode(definition.requestCodec(), false);
                if (v != null) {
                    sink.next(v);
                }
                if (!(v instanceof ReferenceCounted)) {
                    ReferenceCountUtil.safeRelease(req);
                }
                if (req.getType() == RpcRequest.Type.NEXT_AND_END) {
                    sink.complete();
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
                sink.error(e);
            } finally {
                ReferenceCountUtil.safeRelease(req);
            }
        }

    }



    private <REQ, RES> Disposable doListen(RpcDefinition<REQ, RES> definition,
                                           BiFunction<String, Publisher<REQ>, Publisher<RES>> invokeResult) {

        Map<Long, PendingRequest<REQ, RES>> request = new ConcurrentHashMap<>();

        //订阅请求
        return eventBus
                .subscribe(Subscription
                                   .of(definition.getId(),
                                       definition.getAddress(),
                                       Subscription.Feature.local,
                                       Subscription.Feature.broker))
                .map(RpcRequest::parse)
                .doOnCancel(request::clear)
                .subscribe(_req -> request
                        .computeIfAbsent(_req.getRequestId(),
                                         requestId -> new PendingRequest<>(_req.getRequesterId(),
                                                                           requestId,
                                                                           definition,
                                                                           invokeResult, () -> request.remove(requestId)))

                        .next(_req)
                );
    }

}
