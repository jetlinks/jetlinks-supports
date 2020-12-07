package org.jetlinks.supports.ipc;

import lombok.Getter;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.defaults.DirectCodec;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.ipc.IpcCode;
import org.jetlinks.core.ipc.IpcDefinition;
import org.jetlinks.core.ipc.IpcException;
import org.jetlinks.core.ipc.IpcInvoker;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class EventBusIpcRequester<REQ, RES> implements IpcInvoker<REQ, RES> {

    private final int id;//全局ID
    @Getter
    private final String name;
    private final EventBus eventBus;
    private final IpcDefinition<REQ, RES> definition;
    private final Disposable.Composite disposable = Disposables.composite();
    private final Map<Integer, IpcRequestHandler<RES>> pending = new ConcurrentHashMap<>();
    private final AtomicInteger requestIdInc = new AtomicInteger(0);

    private final String sendTopic;

    private final Logger log;

    EventBusIpcRequester(int id,
                         String name,
                         EventBus eventBus,
                         IpcDefinition<REQ, RES> definition) {
        this.id = id;
        this.name = name;
        this.eventBus = eventBus;
        this.definition = definition;
        this.sendTopic = "/_ipc/" + (definition.getAddress().replace("/", "-")) + "/" + name;
        this.log = LoggerFactory.getLogger("ipc.requester." + definition.getAddress() + "." + name);
        init();
    }

    void init() {

        String replyTopic = this.sendTopic + "/" + id + "/_reply";

        String subscriberId = String.join("-", "ipc", String.valueOf(id), name, "handler");

        disposable.add(eventBus
                               .subscribe(Subscription.builder()
                                                      .subscriberId(subscriberId)
                                                      .topics(replyTopic)
                                                      .broker()
                                                      .local()
                                                      .shared()
                                                      .build())
                               .subscribe(this::handleReply)
        );
    }

    @Override
    public Mono<RES> request() {
        return this
                .doRequestWithHandler(RequestType.noArgRequest, null)
                .flatMap(IpcRequestHandler::handleRequest);
    }

    @Override
    public Mono<RES> request(REQ req) {
        return this
                .doRequestWithHandler(RequestType.request, req)
                .flatMap(IpcRequestHandler::handleRequest);
    }

    @Override
    public Flux<RES> requestStream(REQ req) {
        return this
                .doRequestWithHandler(RequestType.requestStream, req)
                .flatMapMany(IpcRequestHandler::handleStream);
    }

    @Override
    public Flux<RES> requestStream() {
        return this
                .doRequestWithHandler(RequestType.noArgRequestStream, null)
                .flatMapMany(IpcRequestHandler::handleStream);
    }

    @Override
    public Flux<RES> requestChannel(Publisher<REQ> req) {
        return this
                .doRequestChannel(req)
                .handleStream();
    }

    @Override
    public Mono<Void> fireAndForget() {
        return this
                .doRequest(RequestType.noArgFireAndForget, 0, null);
    }

    @Override
    public Mono<Void> fireAndForget(REQ req) {
        return this
                .doRequest(RequestType.fireAndForget, 0, req);
    }


    Mono<Void> doRequest(RequestType requestType, int requestId, REQ request) {
        return doRequest(requestType, requestId, -1, request);
    }

    Mono<Void> doRequest(RequestType requestType, int requestId, int seq, REQ request) {
        log.trace("do ipc request {} {}", requestType, requestId);
        return eventBus
                .publish(sendTopic, encodeRequest(requestType, requestId, seq, request))
                .doOnNext(i -> {
                    if (i == 0) {
                        throw new IpcException(IpcCode.ipcServiceUnavailable, "Service " + name + " Unavailable");
                    } else if (i > 1) {
                        log.warn("service {} request {} has multi({}) producer", requestType, request, i);
                    }
                })
                .then();
    }

    IpcRequestHandler<RES> newHandler(int requestId) {
        IpcRequestHandler<RES> handler = new IpcRequestHandler<>();
        pending.put(requestId, handler);
        return handler.doOnDispose(() -> pending.remove(requestId));
    }

    Mono<IpcRequestHandler<RES>> doRequestWithHandler(RequestType requestType, REQ request) {
        int requestId = nextRequestId();
        IpcRequestHandler<RES> handler = newHandler(requestId);
        return this
                .doRequest(requestType, requestId, request)
                .thenReturn(handler);
    }

    IpcRequestHandler<RES> doRequestChannel(Publisher<REQ> channel) {
        int requestId = nextRequestId();
        IpcRequestHandler<RES> handler = newHandler(requestId);
        AtomicInteger seq = new AtomicInteger();
        handler.doOnDispose(eventBus
                                    .publish(sendTopic,
                                             DirectCodec.instance(),
                                             Flux.from(channel)
                                                 .index()
                                                 .map(request -> {
                                                     int sqlVal = request.getT1().intValue();
                                                     seq.set(sqlVal);
                                                     return encodeRequest(RequestType.requestChannel,
                                                                          requestId,
                                                                          sqlVal,
                                                                          request.getT2());
                                                 })
                                                 //todo 负载均衡这里会有问题,cancel可能无法推送到接收流的服务.
                                                 .doFinally(s -> this
                                                         .doRequest(RequestType.cancel, requestId, seq.get(), null)
                                                         .subscribe())

                                    )
                                    .doOnNext(len -> {
                                        if (len == 0) {
                                            handler.error(new IpcException(IpcCode.ipcServiceUnavailable));
                                        }
                                    })
                                    .subscribe()
        );

//        handler.doOnDispose(Flux.from(channel)
//                                .flatMap(req -> this.doRequest(requestType, requestId, req))
//                                .doFinally(s -> this.doRequest(RequestType.cancel, requestId, null)
//                                                    .subscribe())
//                                .doOnError(handler::error)
//                                .subscribe()
//        );
        return handler;
    }

    Payload encodeRequest(RequestType type, int messageId, int seq, REQ data) {
        return Payload.of(IpcRequest.of(type, this.id, messageId, seq, data).toByteBuf(definition.requestCodec()));
    }

    void handleReply(TopicPayload payload) {
        try {
            IpcResponse<RES> response = IpcResponse.decode(payload,
                                                           definition.responseCodec(),
                                                           definition.errorCodec());
            log.trace("handle ipc response {} id:{} seq:{}", response.getType(), response.getMessageId(), response.getSeq());
            IpcRequestHandler<RES> handler = pending.get(response.getMessageId());
            if (handler == null) {
                log.debug("unknown response {}", response);
            } else {
                handler.handle(response);
            }
        } catch (Throwable throwable) {
            log.error("handle response error", throwable);
        }
    }


    public int nextRequestId() {
        int requestId;
        do {
            requestId = requestIdInc.incrementAndGet();
            if (requestId <= 0) {
                requestIdInc.set(requestId = 1);
            }
        } while (pending.containsKey(requestId));
        return requestId;
    }

    @Override
    public void dispose() {
        pending.values().forEach(IpcRequestHandler::dispose);
        pending.clear();
        this.disposable.dispose();
    }
}
