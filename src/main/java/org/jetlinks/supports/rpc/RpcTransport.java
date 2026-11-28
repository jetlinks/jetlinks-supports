package org.jetlinks.supports.rpc;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.netty.server.CloseableChannel;
import reactor.core.publisher.Mono;
import java.net.URI;
import java.util.function.Supplier;

/** Establishes native RSocket endpoints; RPC codecs and exact payload budgets belong to the runtime. */
public interface RpcTransport {
    Mono<CloseableChannel> bind(URI address, int maxPayloadBytes, SocketAcceptor acceptor);
    /** Acquires one owned SETUP payload per subscription; the connector snapshots and releases it. */
    Mono<RSocket> connect(URI address, int maxPayloadBytes, Supplier<Payload> setup, SocketAcceptor acceptor);
    Mono<Void> close();
}
