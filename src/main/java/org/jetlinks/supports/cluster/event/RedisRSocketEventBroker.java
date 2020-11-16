package org.jetlinks.supports.cluster.event;

import io.netty.buffer.Unpooled;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ServerNode;
import org.jetlinks.core.event.TopicPayload;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.rsocket.SocketAcceptor.forRequestStream;

@Slf4j
public class RedisRSocketEventBroker extends RedisClusterEventBroker {

    private final RSocketAddress address;

    private String serverId;

    private final ConcurrentMap<String, RSocket> sockets = new ConcurrentHashMap<>();

    private ClusterCache<String, RSocketAddress> addressCache;

    private final ConcurrentMap<String, RSocketAddress> remotes = new ConcurrentHashMap<>();

    private final Map<String, EmitterProcessor<TopicPayload>> remoteSink = new ConcurrentHashMap<>();
    private final Map<String, EmitterProcessor<TopicPayload>> localSink = new ConcurrentHashMap<>();

    public RedisRSocketEventBroker(ClusterManager clusterManager,
                                   ReactiveRedisConnectionFactory factory,
                                   RSocketAddress address) {
        super(clusterManager, factory);
        this.address = address;
        init();
    }

    public void connectRemote(String remote) {
        if (serverId.equals(remote)) {
            return;
        }
        {
            RSocket socket = sockets.get(remote);
            if (socket != null && !socket.isDisposed()) {
                return;
            }
        }
        EmitterProcessor<TopicPayload> processor = getOrCreateLocalSink(remote);

        RSocketConnector
                .create()
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1))
                                .doBeforeRetry(s -> {
                                    if (s.failure() != null) {
                                        RSocketAddress address = remotes.get(remote);
                                        log.warn("reconnect rsocket event broker {}{}:{}", remote, address, s
                                                .failure()
                                                .getMessage());
                                    }
                                }))
                .connect(() -> {
                    RSocketAddress address = remotes.get(remote);
                    if (address == null) {
                        return null;
                    }
                    return TcpClientTransport.create(address.getPublicAddress(), address.getPublicPort());
                })
                .doOnNext(socket -> {
                    RSocket old = sockets.put(remote, socket);
                    if (old != null && old != socket) {
                        old.dispose();
                    }
                    log.debug("{} start poll broker event from {}", serverId, remote);
                    socket.requestStream(ByteBufPayload.create(serverId))
                          .retryWhen(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
                          .doOnCancel(() -> log.debug("{} cancel poll broker event from {}", serverId, remote))
                          .subscribe(payload -> {
                              try {
                                  if (!processor.hasDownstreams()) {
                                      payload.release();
                                  } else {
                                      String topic = payload.getMetadataUtf8();
                                      processor.onNext(TopicPayload.of(topic, RSocketPayload.of(payload)));
                                  }
                              } catch (Throwable e) {
                                  log.error("handle broker [{}] event error", remote, e);
                                  try {
                                      payload.release();
                                  } catch (Throwable ignore) {
                                  }
                              }
                          });
                })
                .doOnError(err -> log.error("connect to cluster node [{}] error", remote, err))
                .subscribe();
    }

    @Override
    protected void handleServerNodeLeave(ServerNode node) {
        remotes.clear();
        reloadAddresses().subscribe();
    }

    @Override
    protected void handleServerNodeJoin(ServerNode node) {
        if (!serverId.equals(node.getId())) {
            getOrCreateRemoteSink(node.getId());

            addressCache.get(node.getId())
                        .switchIfEmpty(Mono.delay(Duration.ofSeconds(1))
                                           .then(addressCache.get(node.getId())))
                        .subscribe(address -> {
                            remotes.put(node.getId(), address);
                            connectRemote(node.getId());
                        });
        }
    }

    public Mono<Void> reloadAddresses() {
        return addressCache
                .entries()
                .doOnNext(e -> {
                    remotes.put(e.getKey(), e.getValue());
                    connectRemote(e.getKey());
                })
                .then();
    }

    protected Mono<io.rsocket.Payload> topicPayloadToRSocketPayload(TopicPayload payload) {
        try {
            return Mono
                    .just(ByteBufPayload
                                  .create(payload.getBody(),
                                          Unpooled.wrappedBuffer(payload.getTopic().getBytes()))
                    );
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return Mono.empty();
    }

    public void init() {
        this.addressCache = clusterManager.getCache("__rsocket_addresses");
        this.serverId = clusterManager.getCurrentServerId();
        RSocketServer
                .create(forRequestStream(payload ->
                                         {
                                             String broker = payload.getDataUtf8();
                                             log.debug("{} handle broker[{}] event request", serverId, broker);
                                             payload.release();
                                             EmitterProcessor<TopicPayload> processor = getOrCreateRemoteSink(broker);
                                             return processor
                                                     .doOnCancel(() -> log.debug("stop handle broker[{}] event request", broker))
                                                     .flatMap(this::topicPayloadToRSocketPayload)
                                                     ;
                                         }
                ))
                .bind(TcpServerTransport.create(address.getPort()))
                .doOnError(err -> log.error(err.getMessage(), err))
                .block();

        addressCache.put(serverId, address).block(Duration.ofSeconds(10));

        reloadAddresses().block(Duration.ofSeconds(10));

        super.startup();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.addressCache
                .remove(serverId)
                .block();
    }

    @Override
    public void startup() {

    }

    private EmitterProcessor<TopicPayload> getOrCreateRemoteSink(String brokerId) {
        return remoteSink
                .compute(brokerId, (k, val) -> {
                    if (val != null && !val.isCancelled()) {
                        return val;
                    }
                    return EmitterProcessor.create(Integer.MAX_VALUE, false);
                });
    }

    private EmitterProcessor<TopicPayload> getOrCreateLocalSink(String brokerId) {
        return localSink
                .compute(brokerId, (k, val) -> {
                    if (val != null && !val.isCancelled()) {
                        return val;
                    }
                    return EmitterProcessor.create(Integer.MAX_VALUE, false);
                });
    }

    @Override
    protected Flux<TopicPayload> listen(String localId, String brokerId) {
        return Flux.merge(
                this.getOrCreateLocalSink(brokerId)
                ,
                super.listen(localId, brokerId)
        );
    }

    @Override
    protected Mono<Void> dispatch(String localId, String brokerId, TopicPayload payload) {

        EmitterProcessor<TopicPayload> processor = remoteSink.get(brokerId);
        if (processor == null || !processor.hasDownstreams() || processor.isDisposed()) {
            log.debug("no rsocket broker [{}] event listener,fallback to redis", brokerId);
            connectRemote(brokerId);
            return super.dispatch(localId, brokerId, payload);
        }
        processor.onNext(payload);
        return Mono.empty();
    }
}
