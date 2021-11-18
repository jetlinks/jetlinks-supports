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
import org.jetlinks.core.Payload;
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

import java.util.Map;
import java.util.Objects;

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

    public ScalecubeEventBusBroker(ExtendedCluster cluster) {
        this.cluster = cluster;
        this.init();
    }

    private void init() {
        cluster.handler(extendedCluster -> new ClusterMessageHandler() {
            @Override
            public void onMessage(Message message) {
                if (Objects.equals(message.qualifier(), PUB_QUALIFIER)) {
                    String from = message.header(FROM_HEADER);
                    String topic = message.header(TOPIC_HEADER);
                    if (StringUtils.hasText(from) && StringUtils.hasText(topic)) {
                        MemberEventConnection connection = cachedConnections.get(from);
                        if (null != connection) {
                            log.trace("publish from {} : {}", from, topic);
                            connection
                                    .subscriber
                                    .emitNext(
                                            TopicPayload.of(topic, Payload.of((byte[]) message.data())),
                                            RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED
                                    );
                        }
                    }
                } else {
                    Sinks.Many<Subscription> sink = null;
                    String from = message.header(FROM_HEADER);
                    if (StringUtils.hasText(from)) {
                        MemberEventConnection connection = cachedConnections.get(from);
                        if (null != connection) {
                            if (Objects.equals(message.qualifier(), SUB_QUALIFIER)) {
                                log.debug("subscribe from {} : {}", from, message.data());
                                sink = connection.subscriptions;
                            } else if (Objects.equals(message.qualifier(), UNSUB_QUALIFIER)) {
                                log.debug("unsubscribe from {} : {}", from, message.data());
                                sink = connection.unSubscriptions;
                            }
                        }
                    }
                    if (sink != null) {
                        sink.emitNext(message.data(), RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED);
                    }
                }
            }

            @Override
            public void onGossip(Message gossip) {

            }

            @Override
            public void onMembershipEvent(MembershipEvent event) {
                if (event.isLeaving() || event.isRemoved()) {
                    MemberEventConnection connection = cachedConnections.remove(event.member().id());
                    if (connection != null) {
                        log.debug("remove event broker {}",event.member().address());
                        connection.dispose();
                    }
                }
                if (event.isAdded() || event.isUpdated()) {
                    cachedConnections.compute(event.member().id(), (key, old) -> {
                        if (old == null) {
                            log.debug("add event broker {}",event.member().address());
                            MemberEventConnection connection = new MemberEventConnection(event.member());
                            connections.tryEmitNext(connection);
                            return connection;
                        } else {
                            old.setMember(event.member());
                        }
                        return old;
                    });
                }
            }
        });
        for (Member member : cluster.otherMembers()) {
            cachedConnections.put(member.id(), new MemberEventConnection(member));
        }
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
                return cluster
                        .send(member, Message
                                .builder()
                                .qualifier(PUB_QUALIFIER)
                                .header(TOPIC_HEADER, payload.getTopic())
                                .header(FROM_HEADER, cluster.member().id())
                                .data(payload.getBytes())
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
