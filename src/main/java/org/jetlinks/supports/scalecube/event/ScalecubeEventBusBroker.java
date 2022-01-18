package org.jetlinks.supports.scalecube.event;

import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.reactor.RetryNonSerializedEmitFailureHandler;
import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.NativePayload;
import org.jetlinks.core.Payload;
import org.jetlinks.core.cache.Caches;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.supports.event.EventBroker;
import org.jetlinks.supports.event.EventConnection;
import org.jetlinks.supports.event.EventConsumer;
import org.jetlinks.supports.event.EventProducer;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class ScalecubeEventBusBroker implements EventBroker {

    private static final String SUB_QUALIFIER = "/jeb/_sub";
    private static final String UNSUB_QUALIFIER = "/jeb/_unsub";

    private static final String PUB_QUALIFIER = "/jeb/_pub";
    private static final String FROM_HEADER = "_f";
    private static final String TOPIC_HEADER = "_t";

    final ExtendedCluster cluster;

    private final Map<String, MemberEventConnection> cachedConnections = new NonBlockingHashMap<>();
    private final Sinks.Many<EventConnection> connections = Sinks.many().multicast().directBestEffort();

    private final Map<String, List<Message>> earlyMessage = Caches.newCache(Duration.ofMinutes(10));

    public ScalecubeEventBusBroker(ExtendedCluster cluster) {
        this.cluster = cluster;
        this.init();
    }

    private void init() {
        cluster.handler(extendedCluster -> new ClusterMessageHandler() {
            @Override
            public void onMessage(Message message) {
                String from = message.header(FROM_HEADER);
                if (StringUtils.isEmpty(from)) {
                    return;
                }
                MemberEventConnection connection = getOrCreateConnection(from);
                if (null != connection) {
                    handleMessage(connection, message);
                } else {
                    log.info("received early message {} {}", from, message.data());
                    //收到了消息，但是本地还有没Member信息
                    earlyMessage
                            .computeIfAbsent(from, (id) -> new CopyOnWriteArrayList<>())
                            .add(message);
                }

            }

            @Override
            public void onGossip(Message gossip) {

            }

            @Override
            public void onMembershipEvent(MembershipEvent event) {
                if (event.isLeaving() || event.isRemoved()) {
                    earlyMessage.remove(event.member().id());
                    MemberEventConnection connection = cachedConnections.remove(event.member().id());
                    if (connection != null) {
                        log.debug("remove event broker {}", event.member().address());
                        connection.dispose();
                    }
                }
                if (event.isAdded() || event.isUpdated()) {
                    getOrCreateConnection(event.member());
                }
            }
        });
        for (Member member : cluster.otherMembers()) {
            cachedConnections.put(member.id(), new MemberEventConnection(member));
        }
    }

    private MemberEventConnection getOrCreateConnection(String memberId) {
        Member member = cluster.member(memberId).orElse(null);
        return member == null ? null : getOrCreateConnection(member);
    }

    private void handleMessage(MemberEventConnection connection, Message message) {
        if (Objects.equals(message.qualifier(), PUB_QUALIFIER)) {
            String topic = message.header(TOPIC_HEADER);
            Object data = message.data();
            TopicPayload topicPayload;
            if (data instanceof byte[]) {
                topicPayload = TopicPayload.of(topic, Payload.of((byte[]) message.data()));
            } else {
                topicPayload = TopicPayload.of(topic, NativePayload.of(data));
            }
            log.trace("publish from {} : {}", connection, topic);
            connection
                    .subscriber
                    .emitNext(topicPayload,
                              RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED
                    );
        }
        if (Objects.equals(message.qualifier(), SUB_QUALIFIER)) {
            log.debug("subscribe from {} : {}", connection, message.data());
            connection.subscriptions.tryEmitNext(message.data());
        } else if (Objects.equals(message.qualifier(), UNSUB_QUALIFIER)) {
            log.debug("unsubscribe from {} : {}", connection, message.data());
            connection.unSubscriptions.tryEmitNext(message.data());
        }
    }

    private MemberEventConnection getOrCreateConnection(Member member) {
        return cachedConnections.compute(member.id(), (key, old) -> {
            if (old == null) {
                log.debug("add event broker {}", member.address());
                MemberEventConnection connection = new MemberEventConnection(member);
                connections.emitNext(connection, (signalType, emitResult) -> {
                    return emitResult == Sinks.EmitResult.FAIL_NON_SERIALIZED
                            || emitResult == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER;
                });
                List<Message> early = earlyMessage.get(member.id());
                if (null != early) {
                    for (Message message : early) {
                        handleMessage(connection, message);
                    }
                }
                return connection;
            } else {
                old.setMember(member);
            }
            return old;
        });
    }

    @AllArgsConstructor
    public class MemberEventConnection implements EventConnection, EventProducer, EventConsumer {
        @Setter
        private Member member;
        private final Sinks.Many<Subscription> subscriptions = Sinks.many().multicast().directBestEffort();
        private final Sinks.Many<Subscription> unSubscriptions = Sinks.many().multicast().directBestEffort();

        private final Sinks.Many<TopicPayload> subscriber = Sinks
                .many()
                .multicast()
                .onBackpressureBuffer(Integer.MAX_VALUE, false);

        private final Disposable.Composite disposable = Disposables.composite();

        private FluxSink<TopicPayload> publisher;

        public MemberEventConnection(Member member) {
            this.member = member;
            doOnDispose(Flux.<TopicPayload>create(sink -> {
                                publisher = sink;
                            })
                            .flatMap(this::doPublish)
                            .subscribe()
            );
        }

        private Mono<Void> doPublish(TopicPayload payload) {
            try {
                String topic = payload.getTopic();
                Object payloadObj;
                if (payload.getPayload() instanceof NativePayload) {
                    payloadObj = ((NativePayload<?>) payload.getPayload()).getNativeObject();
                    payload.release();
                } else {
                    payloadObj = payload.getBytes();
                }
                return cluster
                        .send(member, Message
                                .builder()
                                .qualifier(PUB_QUALIFIER)
                                .header(TOPIC_HEADER, topic)
                                .header(FROM_HEADER, cluster.member().id())
                                .data(payloadObj)
                                .build())
                        .onErrorResume(err -> {
                            log.error(err.getMessage(), err);
                            return Mono.empty();
                        });
            } catch (Throwable err) {
                log.error(err.getMessage(), err);
                return Mono.empty();
            }
        }

        @Override
        public String getId() {
            return member.id();
        }

        @Override
        public boolean isAlive() {
            return cluster.member(member.id()).isPresent();
        }

        @Override
        public void doOnDispose(Disposable disposable) {
            this.disposable.add(disposable);
        }

        @Override
        public EventBroker getBroker() {
            return ScalecubeEventBusBroker.this;
        }

        @Override
        public Flux<Subscription> handleSubscribe() {
            return subscriptions.asFlux();
        }

        @Override
        public Flux<Subscription> handleUnSubscribe() {
            return unSubscriptions.asFlux();
        }

        @Override
        public FluxSink<TopicPayload> sink() {
            return publisher;
        }

        private Message toMessage(String qualifier, Subscription subscription) {
            return Message
                    .builder()
                    .qualifier(qualifier)
                    .data(subscription)
                    .header(FROM_HEADER, cluster.member().id())
                    .build();
        }

        @Override
        public Mono<Void> subscribe(Subscription subscription) {
            return cluster
                    .send(member, toMessage(SUB_QUALIFIER, subscription))
                    .then();
        }

        @Override
        public Mono<Void> unsubscribe(Subscription subscription) {
            return cluster
                    .send(member, toMessage(UNSUB_QUALIFIER, subscription))
                    .then();
        }

        @Override
        public Flux<TopicPayload> subscribe() {
            return subscriber.asFlux();
        }

        @Override
        public void dispose() {
            disposable.dispose();
        }

        @Override
        public String toString() {
            return member.alias()+"@"+member.address();
        }
    }

    @Override
    public String getId() {
        return "scalecube";
    }

    @Override
    public Flux<EventConnection> accept() {
        return Flux.concat(Flux.fromIterable(cachedConnections.values()), connections.asFlux());
    }

}
