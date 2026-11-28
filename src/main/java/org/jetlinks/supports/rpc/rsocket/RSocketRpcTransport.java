package org.jetlinks.supports.rpc.rsocket;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import org.jetlinks.supports.rpc.RpcTransport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Native RSocket TCP endpoints with ZERO_COPY decoding and owned I/O resources. */
public final class RSocketRpcTransport implements RpcTransport {
    private static final int FRAGMENT_SIZE = 16 * 1024;
    private static final int REASSEMBLY_OVERHEAD = FrameHeaderCodec.size() + Integer.BYTES + 2 * FrameLengthCodec.FRAME_LENGTH_SIZE;
    private final LoopResources loops;
    private final ByteBufAllocator allocator;
    private final Set<RSocket> sockets = ConcurrentHashMap.newKeySet();
    private final Set<CloseableChannel> servers = ConcurrentHashMap.newKeySet();

    public RSocketRpcTransport(int workers) { this(workers, ByteBufAllocator.DEFAULT); }
    public RSocketRpcTransport(int workers, ByteBufAllocator allocator) {
        loops = LoopResources.create("jetlinks-rpc", workers, true);
        this.allocator = allocator;
    }
    @Override
    public Mono<CloseableChannel> bind(URI address, int maxPayloadBytes, SocketAcceptor acceptor) {
        return RSocketServer.create((setup, reverse) -> {
                track(reverse);
                return acceptor.accept(setup, reverse);
            }).payloadDecoder(PayloadDecoder.ZERO_COPY).fragment(FRAGMENT_SIZE)
            .maxInboundPayloadSize(inboundLimit(maxPayloadBytes))
            .bind(TcpServerTransport.create(TcpServer.create().host(address.getHost()).port(address.getPort()).runOn(loops)
                .childOption(ChannelOption.ALLOCATOR, allocator), FRAGMENT_SIZE))
            .doOnNext(servers::add);
    }
    @Override
    public Mono<RSocket> connect(URI address, int maxPayloadBytes, Supplier<Payload> setup, SocketAcceptor acceptor) {
        return Mono.defer(() -> RSocketConnector.create().payloadDecoder(PayloadDecoder.ZERO_COPY)
            .fragment(FRAGMENT_SIZE).maxInboundPayloadSize(inboundLimit(maxPayloadBytes)).acceptor(acceptor)
            .setupPayload(setup.get())
            .connect(TcpClientTransport.create(TcpClient.create().host(address.getHost()).port(address.getPort()).runOn(loops)
                .option(ChannelOption.ALLOCATOR, allocator), FRAGMENT_SIZE))
            .doOnNext(this::track));
    }
    private static int inboundLimit(int maxPayloadBytes) {
        // A small application budget must still admit one physical frame before the runtime checks its exact size.
        return (int) Math.max(FRAGMENT_SIZE, Math.min(Integer.MAX_VALUE, (long) maxPayloadBytes + REASSEMBLY_OVERHEAD));
    }
    private void track(RSocket socket) {
        sockets.add(socket);
        socket.onClose().subscribe(ignored -> {}, error -> sockets.remove(socket), () -> sockets.remove(socket));
    }
    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            servers.forEach(CloseableChannel::dispose);
            sockets.forEach(RSocket::dispose);
            servers.clear(); sockets.clear();
            return loops.disposeLater();
        });
    }
}
