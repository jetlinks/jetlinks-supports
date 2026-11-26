package org.jetlinks.supports.rpc.rsocket;

import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import org.jetlinks.core.rpc.transport.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.Function;

/** 原生 RSocket TCP 传输；ZERO_COPY 接收，单连接支持反向调用，拥有自己的 I/O loops。 */
public final class RSocketRpcTransport implements RpcTransport {
    private static final int FRAGMENT_SIZE = 16 * 1024;
    // Initial header/requestN/metadata length, plus the following fragment's metadata length during reassembly.
    private static final int REASSEMBLY_OVERHEAD = FrameHeaderCodec.size() + Integer.BYTES + 2 * FrameLengthCodec.FRAME_LENGTH_SIZE;
    private final LoopResources loops;
    private final ByteBufAllocator allocator;
    private final Set<RSocket> sockets = ConcurrentHashMap.newKeySet();
    private final Set<RpcServer> servers = ConcurrentHashMap.newKeySet();

    public RSocketRpcTransport(int workers) {
        this(workers, ByteBufAllocator.DEFAULT);
    }
    public RSocketRpcTransport(int workers, ByteBufAllocator allocator) {
        loops = LoopResources.create("jetlinks-rpc", workers, true);
        this.allocator = allocator;
    }

    @Override
    public Mono<RpcServer> bind(URI address, int maxPayloadBytes, Acceptor acceptor) {
        int limit = inboundLimit(maxPayloadBytes);
        return RSocketServer.create((setup, reverse) -> Mono.using(() -> decode(setup.retain(), maxPayloadBytes),
                frame -> acceptor.accept(frame, connection(reverse, maxPayloadBytes)).map(target -> handler(target, maxPayloadBytes)),
                RpcFrame::release, false))
            .payloadDecoder(PayloadDecoder.ZERO_COPY).fragment(FRAGMENT_SIZE).maxInboundPayloadSize(limit)
            .bind(TcpServerTransport.create(TcpServer.create().host(address.getHost()).port(address.getPort()).runOn(loops)
                .childOption(ChannelOption.ALLOCATOR, allocator), FRAGMENT_SIZE))
            .map(channel -> {
                RpcServer server = new RpcServer() {
                    public URI address() { return URI.create("tcp://" + channel.address().getHostString() + ":" + channel.address().getPort()); }
                    public Mono<Void> close() { return Mono.defer(() -> { channel.dispose(); servers.remove(this); return channel.onClose(); }); }
                };
                servers.add(server);
                return server;
            });
    }

    @Override
    public Mono<RpcConnection> connect(URI address, int maxPayloadBytes, Supplier<RpcFrame> setup, RpcHandler inbound) {
        int limit = inboundLimit(maxPayloadBytes);
        return Mono.defer(() -> RSocketConnector.create().payloadDecoder(PayloadDecoder.ZERO_COPY)
            .fragment(FRAGMENT_SIZE).maxInboundPayloadSize(limit).acceptor((ignored, reverse) -> Mono.just(handler(inbound, maxPayloadBytes)))
            // The documented SETUP overload makes one immutable connection-level snapshot for safe cancellation/reuse.
            .setupPayload(encode(setup.get()))
            .connect(TcpClientTransport.create(TcpClient.create().host(address.getHost()).port(address.getPort()).runOn(loops)
                .option(ChannelOption.ALLOCATOR, allocator), FRAGMENT_SIZE))
            .map(socket -> connection(socket, maxPayloadBytes)));
    }

    private static int inboundLimit(int maxPayloadBytes) {
        // Peers share this MTU; a smaller application budget must still admit one peer frame before exact decoding.
        return (int) Math.max(FRAGMENT_SIZE, Math.min(Integer.MAX_VALUE, (long) maxPayloadBytes + REASSEMBLY_OVERHEAD));
    }

    private RpcConnection connection(RSocket socket, int maxPayloadBytes) {
        sockets.add(socket);
        Function<Payload, RpcFrame> decoder = payload -> decode(payload, maxPayloadBytes);
        socket.onClose().subscribe(ignored -> {}, error -> sockets.remove(socket), () -> sockets.remove(socket));
        return new RpcConnection() {
            public Mono<RpcFrame> requestResponse(Supplier<RpcFrame> request) {
                return Mono.defer(() -> socket.requestResponse(encode(request.get())).map(decoder));
            }
            public Flux<RpcFrame> requestStream(Supplier<RpcFrame> request) {
                return Flux.defer(() -> socket.requestStream(encode(request.get())).map(decoder));
            }
            public Flux<RpcFrame> requestChannel(Publisher<RpcFrame> requests) {
                return Flux.defer(() -> socket.requestChannel(Flux.from(requests)
                    .map(RSocketRpcTransport::encode).doOnDiscard(RpcFrame.class, RpcFrame::release))
                    .map(decoder));
            }
            public Mono<Void> fireAndForget(Supplier<RpcFrame> request) {
                return Mono.defer(() -> socket.fireAndForget(encode(request.get())));
            }
            public Mono<Void> onClose() { return socket.onClose(); }
            public Mono<Void> close() {
                // onClose reports the connection failure; cleanup itself remains idempotent after that failure.
                return Mono.defer(() -> { socket.dispose(); sockets.remove(socket); return socket.onClose().onErrorComplete(); });
            }
        };
    }

    private static RSocket handler(RpcHandler target, int maxPayloadBytes) {
        return new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) {
                return Mono.using(() -> decode(payload, maxPayloadBytes), target::requestResponse, RpcFrame::release, false)
                    .map(RSocketRpcTransport::encode);
            }
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.using(() -> decode(payload, maxPayloadBytes), target::requestStream, RpcFrame::release, false)
                    .map(RSocketRpcTransport::encode);
            }
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return target.requestChannel(Flux.from(payloads).map(payload -> decode(payload, maxPayloadBytes))).map(RSocketRpcTransport::encode);
            }
            public Mono<Void> fireAndForget(Payload payload) {
                return Mono.using(() -> decode(payload, maxPayloadBytes), target::fireAndForget, RpcFrame::release, false);
            }
        };
    }

    private static Payload encode(RpcFrame frame) {
        try {
            return ByteBufPayload.create(frame.data().retainedSlice(), frame.metadata().retainedSlice());
        } finally { frame.release(); }
    }

    private static RpcFrame decode(Payload payload, int maxPayloadBytes) {
        try {
            if ((long) payload.data().readableBytes() + (payload.hasMetadata() ? payload.metadata().readableBytes() : 0) > maxPayloadBytes) {
                throw new IllegalArgumentException("RPC payload too large");
            }
            return new RpcFrame(payload.data().retainedSlice(), payload.hasMetadata()
                ? payload.metadata().retainedSlice() : Unpooled.EMPTY_BUFFER);
        } finally { payload.release(); }
    }

    @Override
    public Mono<Void> close() {
        return Flux.fromIterable(servers).concatMap(RpcServer::close).then(Mono.fromRunnable(() -> {
            sockets.forEach(RSocket::dispose);
            sockets.clear();
        })).then(loops.disposeLater());
    }
}
