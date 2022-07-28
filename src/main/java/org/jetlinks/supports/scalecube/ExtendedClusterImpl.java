package org.jetlinks.supports.scalecube;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class ExtendedClusterImpl implements ExtendedCluster {

    private final static String FEATURE_QUALIFIER = "_c_fts_q";
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

    private final Map<String, Set<String>> featureMembers = new ConcurrentHashMap<>();

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
            messageSink.emitNext(message, Reactors.emitFailureHandler());
            doHandler(message, ClusterMessageHandler::onMessage);
        }

        @Override
        public void onGossip(Message gossip) {
            if (FEATURE_QUALIFIER.equals(gossip.qualifier())) {
                String from = gossip.header(FEATURE_FROM);
                member(from).ifPresent(mem -> addFeature(mem, gossip.data()));
                return;
            }
            messageSink.emitNext(gossip, Reactors.emitFailureHandler());
            doHandler(gossip, ClusterMessageHandler::onGossip);
        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            membershipEvents.emitNext(event, Reactors.emitFailureHandler());
            doHandler(event, ClusterMessageHandler::onMembershipEvent);
            if (event.isRemoved() || event.isLeaving()) {
                removeFeature(event.member());
            }
            //新节点上线,广播自己的feature
            if (event.isAdded()) {
                broadcastFeature().subscribe();
            }
        }
    }

    private void addFeature(Member member, Collection<String> features) {
        if (CollectionUtils.isEmpty(features)) {
            return;
        }
        log.debug("register cluster [{}] feature:{}", member.alias() == null ? member.id() : member.alias(), features);
        for (String feature : features) {
            Set<String> members = featureMembers.computeIfAbsent(feature, (k) -> new ConcurrentHashMap<String, String>().keySet(k));
            members.add(member.id());
            if (StringUtils.hasText(member.alias())) {
                members.add(member.alias());
            }
        }
    }

    private void removeFeature(Member member) {
        for (Set<String> value : featureMembers.values()) {
            if (value.remove(member.id())) {
                log.debug("remove cluster [{}] features", member.alias() == null ? member.id() : member.alias());
            }
            if (StringUtils.hasText(member.alias())) {
                value.remove(member.alias());
            }
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
                //广播特性
                .then(Mono.defer(this::broadcastFeature))
                .then(Flux.fromIterable(startThen)
                          .flatMap(Function.identity())
                          .then(Mono.fromRunnable(startThen::clear)))
                .then(Mono.fromRunnable(this::startBroadcastFeature))
                .thenReturn(this);
    }

    public ExtendedCluster startAwait() {
        start().block();
        return this;
    }

    private Mono<Void> broadcastFeature() {
        return this
                .spreadGossip(Message
                                      .builder()
                                      .qualifier(FEATURE_QUALIFIER)
                                      .header(FEATURE_FROM, member().id())
                                      .data(localFeatures)
                                      .build())
                .then();
    }

    private void startBroadcastFeature() {
        addFeature(member(), localFeatures);
        disposable.add(
                Flux.interval(Duration.ofSeconds(10), Duration.ofSeconds(30))
                    .flatMap(l -> this
                            .broadcastFeature()
                            .onErrorResume(err -> Mono.empty()))
                    .subscribe()
        );
    }

    @Override
    public Flux<MembershipEvent> listenMembership() {
        return membershipEvents.asFlux();
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
        if (TraceHolder.isEnabled()) {
            return TraceHolder
                    .writeContextTo(Message.with(message), Message.Builder::header)
                    .flatMap(msg -> real.send(member, msg.build()));
        }
        return real.send(member, message);
    }

    @Override
    public Mono<Void> send(Address address, Message message) {
        if (TraceHolder.isEnabled()) {
            return TraceHolder
                    .writeContextTo(Message.with(message), Message.Builder::header)
                    .flatMap(msg -> real.send(address, msg.build()));
        }
        return real.send(address, message);
    }

    @Override
    public Mono<Message> requestResponse(Address address, Message request) {
        if (TraceHolder.isEnabled()) {
            return TraceHolder
                    .writeContextTo(Message.with(request), Message.Builder::header)
                    .flatMap(msg -> real.requestResponse(address, request));
        }
        return real.requestResponse(address, request);
    }

    @Override
    public Mono<Message> requestResponse(Member member, Message request) {
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
        real.shutdown();
    }

    @Override
    public Mono<Void> onShutdown() {
        return real.onShutdown();
    }

    @Override
    public boolean isShutdown() {
        return real.isShutdown();
    }

    @Override
    public void registerFeatures(Collection<String> feature) {
        localFeatures.addAll(feature);
    }

    @Override
    public List<Member> featureMembers(String feature) {
        Set<String> members = featureMembers.get(feature);
        if (CollectionUtils.isEmpty(members)) {
            return Collections.emptyList();
        }
        Collection<Member> other = otherMembers();

        List<Member> supports = new ArrayList<>(other.size() + 1);

        for (Member member : other) {
            if (members.contains(member.id())) {
                supports.add(member);
            }
        }
        if (members.contains(member().id())) {
            supports.add(member());
        }
        return supports;
    }

    @Override
    public boolean supportFeature(String member, String featureId) {
        Set<String> members = featureMembers.get(featureId);
        if (CollectionUtils.isEmpty(members)) {
            return false;
        }
        return members.contains(member);
    }
}
