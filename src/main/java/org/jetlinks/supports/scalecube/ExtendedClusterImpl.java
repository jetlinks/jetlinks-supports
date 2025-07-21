package org.jetlinks.supports.scalecube;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class ExtendedClusterImpl implements ExtendedCluster {

    private final static String FEATURE_FROM = "_c_fts_f";

    private final ClusterImpl real;
    private final Sinks.Many<Message> messageSink = Sinks.many().multicast().directBestEffort();
    private final Sinks.Many<Message> gossipSink = Sinks.many().multicast().directBestEffort();
    private final Sinks.Many<MembershipEvent> membershipEvents = Sinks.many().multicast().directBestEffort();

    private final List<ClusterMessageHandler> handlers = new CopyOnWriteArrayList<>();

    private volatile boolean started;
    private final List<Mono<Void>> startThen = new CopyOnWriteArrayList<>();
    //缓存消息
    private final List<Message> messageCache = new CopyOnWriteArrayList<>();
    private long cacheEndWithTime;

    private final List<String> localFeatures = new CopyOnWriteArrayList<>();

    private final Disposable.Composite disposable = Disposables.composite();


    public ExtendedClusterImpl(ClusterConfig config) {
        this(new ClusterImpl(config));
    }

    public ExtendedClusterImpl(ClusterImpl impl) {
        real = impl
            .handler(cluster -> new ClusterMessageHandlerDispatcher());
    }

    class ClusterMessageHandlerDispatcher implements ClusterMessageHandler {
        @Override
        public void onMessage(Message message) {
            if (System.currentTimeMillis() <= cacheEndWithTime && messageCache.size() < 2048) {
                messageCache.add(message);
            }
            try {
                if (messageSink.currentSubscriberCount() > 0) {
                    messageSink.emitNext(message, Reactors.emitFailureHandler());
                }
            } catch (Throwable ignore) {

            }
            doHandler(message, ClusterMessageHandler::onMessage);
        }

        @Override
        public void onGossip(Message gossip) {
            try {
                if (gossipSink.currentSubscriberCount() > 0) {
                    gossipSink.emitNext(gossip, Reactors.emitFailureHandler());
                }
            } catch (Throwable ignore) {
            }

            doHandler(gossip, ClusterMessageHandler::onGossip);
        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            membershipEvents.emitNext(event, Reactors.emitFailureHandler());
            doHandler(event, ClusterMessageHandler::onMembershipEvent);
        }
    }


    private <T> void doHandler(T e, BiConsumer<ClusterMessageHandler, T> consumer) {
        for (ClusterMessageHandler handler : handlers) {
            consumer.accept(handler, e);
        }
    }

    public ExtendedClusterImpl handler(Function<ExtendedCluster, ClusterMessageHandler> handlerFunction) {
        ClusterMessageHandler handler = handlerFunction.apply(this);
        handlers.add(handler);
        writeCacheMessage(handler);
        return this;
    }

    @Override
    public ExtendedClusterImpl handler(ClusterMessageHandler handler) {
        handlers.add(handler);
        writeCacheMessage(handler);
        return this;
    }

    private void writeCacheMessage(ClusterMessageHandler handler) {
        for (Message message : messageCache) {
            handler.onMessage(message);
        }
    }

    public Mono<ExtendedCluster> start() {
        started = true;
        cacheEndWithTime = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        return real
            .start()
            .then(Flux.fromIterable(startThen)
                      .flatMap(Function.identity())
                      .then(Mono.fromRunnable(startThen::clear)))
            .thenReturn(this);
    }

    public ExtendedCluster startAwait() {
        start().block();
        return this;
    }

    @Override
    public Flux<MembershipEvent> listenMembership() {
        return membershipEvents
            .asFlux()
            .onBackpressureBuffer();
    }

    @Override
    public Disposable listenMessage(@Nonnull String qualifier, BiFunction<Message, ExtendedCluster, Mono<Void>> handler) {
        return listen(messageSink, qualifier, handler);
    }

    @Override
    public Disposable listenGossip(@Nonnull String qualifier, BiFunction<Message, ExtendedCluster, Mono<Void>> handler) {
        return listen(gossipSink, qualifier, handler);
    }

    private Disposable listen(Sinks.Many<Message> sink, @Nonnull String qualifier, BiFunction<Message, ExtendedCluster, Mono<Void>> handler) {
        return sink
            .asFlux()
            .onBackpressureBuffer()
            .filter(msg -> Objects.equals(qualifier, msg.qualifier()))
            .flatMap(msg -> handler
                .apply(msg, this)
                .contextWrite(TraceHolder.readToContext(Context.empty(), msg.headers()))
                .onErrorResume(err -> {
                    log.error(err.getMessage(), err);
                    return Mono.empty();
                }))
            .subscribe();
    }

    @Override
    public Address address() {
        return real.address();
    }

    @Override
    public Mono<Void> send(Member member, Message message) {
        if (!real.member(member.address()).isPresent()) {
            return Mono.empty();
        }
        if (TraceHolder.isEnabled()) {
            return TraceHolder
                .writeContextTo(Message.with(message), Message.Builder::header)
                .flatMap(msg -> real.send(member, msg.build()));
        }
        return real.send(member, message);
    }

    @Override
    public Mono<Void> send(Address address, Message message) {
        if (!real.member(address).isPresent()) {
            return Mono.empty();
        }

        if (TraceHolder.isEnabled()) {
            return TraceHolder
                .writeContextTo(Message.with(message), Message.Builder::header)
                .flatMap(msg -> real.send(address, msg.build()));
        }
        return real.send(address, message);
    }

    @Override
    public Mono<Message> requestResponse(Address address, Message request) {
        if (!real.member(address).isPresent()) {
            return Mono.empty();
        }
        if (TraceHolder.isEnabled()) {
            return TraceHolder
                .writeContextTo(Message.with(request), Message.Builder::header)
                .flatMap(msg -> real.requestResponse(address, request));
        }
        return real.requestResponse(address, request);
    }

    @Override
    public Mono<Message> requestResponse(Member member, Message request) {
        if (!real.member(member.address()).isPresent()) {
            return Mono.empty();
        }
        if (TraceHolder.isEnabled()) {
            return TraceHolder
                .writeContextTo(Message.with(request), Message.Builder::header)
                .flatMap(msg -> real.requestResponse(member, request));
        }
        return real.requestResponse(member, request);
    }

    @Override
    public Mono<String> spreadGossip(Message message) {
        if (TraceHolder.isEnabled()) {
            return TraceHolder
                .writeContextTo(Message.with(message), Message.Builder::header)
                .flatMap(msg -> real.spreadGossip(message));
        }
        return real.spreadGossip(message);
    }

    @Override
    public <T> Optional<T> metadata() {
        return real.metadata();
    }

    @Override
    public <T> Optional<T> metadata(Member member) {
        return real.metadata(member);
    }

    @Override
    public Member member() {
        return real.member();
    }

    @Override
    public Optional<Member> member(String id) {
        return real.member(id);
    }

    @Override
    public Optional<Member> member(Address address) {
        return real.member(address);
    }

    @Override
    public Collection<Member> members() {
        return real.members();
    }

    @Override
    public Collection<Member> otherMembers() {
        return real.otherMembers();
    }

    @Override
    public <T> Mono<Void> updateMetadata(T metadata) {
        if (!started) {
            startThen.add(real.updateMetadata(metadata));
            return null;
        }
        return real.updateMetadata(metadata);
    }

    @Override
    public void shutdown() {
        disposable.dispose();
        real.shutdown();
    }

    @Override
    public Mono<Void> onShutdown() {
        return real.onShutdown();
    }

    @Override
    public boolean isShutdown() {
        return disposable.isDisposed();
    }

    @Override
    public void registerFeatures(Collection<String> feature) {
        log.warn("Features unsupported now",new RuntimeException());
    }

    @Override
    public List<Member> featureMembers(String feature) {
        return Collections.emptyList();
    }

    @Override
    public boolean supportFeature(String member, String featureId) {
         return false;
    }
}
