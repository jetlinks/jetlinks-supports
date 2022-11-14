package org.jetlinks.supports.scalecube.event;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.NativePayload;
import org.jetlinks.core.Payload;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.event.TopicPayload;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.RpcService;
import org.jetlinks.core.rpc.ServiceEvent;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.core.utils.SerializeUtils;
import org.jetlinks.supports.event.EventBroker;
import org.jetlinks.supports.event.EventConnection;
import org.jetlinks.supports.event.EventConsumer;
import org.jetlinks.supports.event.EventProducer;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Slf4j
public class ClusterEventBusBroker implements EventBroker, Disposable {

    private final RpcManager rpcManager;

    private final Map<String, RpcEventConnection> connections = new ConcurrentHashMap<>();

    private final Sinks.Many<EventConnection> acceptSink = Reactors.createMany();

    public ClusterEventBusBroker(RpcManager rpcManager) {
        this.rpcManager = rpcManager;

        rpcManager.listen(Api.class)
                  .subscribe(this::handleServiceEvent);

        rpcManager.getServices(Api.class)
                  .subscribe(this::handleService);

        rpcManager.registerService(new ApiImpl());

    }

    private void handleServiceEvent(ServiceEvent event) {
        if (event.getType() == ServiceEvent.Type.removed) {
            disposeConnection(connections.remove(event.getServerNodeId()));
        } else {
            rpcManager
                    .getService(event.getServerNodeId(), Api.class)
                    .subscribe(api -> handleService(event, api));
        }
    }

    protected void handleService(ServiceEvent event, Api api) {
        try {
            RpcEventConnection conn = new RpcEventConnection(event.getServerNodeId(), api);
            RpcEventConnection old = connections.put(event.getServerNodeId(), conn);
            if (old != null) {
                disposeConnection(old);
            } else if (acceptSink.currentSubscriberCount() > 0) {
                acceptSink.emitNext(conn, Reactors.emitFailureHandler());
            }
        } catch (Throwable err) {
            log.warn("register service error {}", event.getServiceId(), err);
        }
    }

    protected void handleService(RpcService<Api> service) {
        try {
            RpcEventConnection conn = new RpcEventConnection(service.serverNodeId(), service.service());
            RpcEventConnection old = connections.put(service.serverNodeId(), conn);
            if (old != null) {
                disposeConnection(old);
            } else if (acceptSink.currentSubscriberCount() > 0) {
                acceptSink.emitNext(conn, Reactors.emitFailureHandler());
            }
        } catch (Throwable err) {
            log.warn("register service error {}", service.serverNodeId(), err);
        }
    }

    private void disposeConnection(RpcEventConnection connection) {
        if (null != connection) {
            connection.dispose();
        }
    }

    @Override
    public String getId() {
        return "rpc-cluster-broker";
    }

    @Override
    public Flux<EventConnection> accept() {
        return Flux.concat(
                Flux.fromIterable(connections.values()),
                acceptSink.asFlux()
        );
    }

    @Override
    public void dispose() {
        connections.values().forEach(this::disposeConnection);
    }


    @SneakyThrows
    protected ObjectOutput createOutput(ByteBuf buf) {
        return new ObjectOutputStream(new ByteBufOutputStream(buf));
    }

    @SneakyThrows
    protected ObjectInput createInput(ByteBuf buf) {
        return new ObjectInputStream(new ByteBufInputStream(buf, true));
    }

    @AllArgsConstructor
    private class RpcEventConnection implements EventConnection, EventProducer, EventConsumer {

        private final String id;
        private final Api api;

        private final Disposable.Composite disposable = Disposables.composite();

        private final Sinks.Many<TopicPayload> consumer = Reactors.createMany(Integer.MAX_VALUE, false);

        private final Sinks.Many<TopicPayload> producer = Reactors.createMany(Integer.MAX_VALUE, false);

        private final Sinks.Many<Subscription> subscriptions = Reactors.createMany(Integer.MAX_VALUE, false);

        private final Sinks.Many<Subscription> unSubscriptions = Reactors.createMany(Integer.MAX_VALUE, false);

        private FluxSink<TopicPayload> sink;

        public RpcEventConnection(String id, Api api) {
            this.id = id;
            this.api = api;
            doOnDispose(
                    Flux.<TopicPayload>create(sink -> {
                            this.sink = sink;
                        })
                        .flatMap(payload -> {
                            ByteBuf buf = this.encodePayload(payload);
                            if (null != buf) {
                                return this.api.pub(buf);
                            }
                            return Mono.empty();

                        })
                        .subscribe()
            );
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean isAlive() {
            return true;
        }

        @Override
        public void doOnDispose(Disposable disposable) {
            this.disposable.add(disposable);
        }

        @Override
        public EventBroker getBroker() {
            return ClusterEventBusBroker.this;
        }

        @Override
        public void dispose() {
            disposable.dispose();
            subscriptions.tryEmitComplete();
            unSubscriptions.tryEmitComplete();
            consumer.tryEmitComplete();
            sink.complete();
        }

        @Override
        public boolean isDisposed() {
            return disposable.isDisposed();
        }

        @Override
        public Mono<Void> subscribe(Subscription subscription) {
            return api.sub(encodeSubscription(subscription));
        }

        @Override
        public Mono<Void> unsubscribe(Subscription subscription) {
            return api.unsub(encodeSubscription(subscription));
        }

        @Override
        public Flux<TopicPayload> subscribe() {
            return producer.asFlux();
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
            return sink;
        }


        @SneakyThrows
        private ByteBuf encodePayload(TopicPayload payload) {
            ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
            try (ObjectOutput output = createOutput(buf)) {
                output.writeUTF(rpcManager.currentServerId());
                output.writeUTF(payload.getTopic());
                SerializeUtils.writeKeyValue(payload.getHeaders(), output);
                Payload p = payload.getPayload();
                if (p instanceof NativePayload) {
                    SerializeUtils.writeObject(((NativePayload<?>) p).getNativeObject(), output);
                } else {
                    output.write(p.getBytes());
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }

            return buf;
        }

        @SneakyThrows
        private ByteBuf encodeSubscription(Subscription payload) {
            ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
            try (ObjectOutput output = createOutput(buf)) {
                output.writeUTF(rpcManager.currentServerId());
                payload.writeExternal(output);
            }
            return buf;

        }
    }

    public class ApiImpl implements Api {

        @SneakyThrows
        private void handleSubs(ByteBuf buf, BiConsumer<RpcEventConnection, Subscription> consumer) {

            try (ObjectInput input = createInput(buf)) {
                String serviceId = input.readUTF();
                RpcEventConnection connection = connections.get(serviceId);
                if (null != connection) {
                    Subscription subscription = new Subscription();
                    subscription.readExternal(input);
                    consumer.accept(connection, subscription);
                }
            } catch (Throwable error) {
                log.error("Error handling subscription", error);
            }
        }

        @Override
        public Mono<Void> sub(ByteBuf sub) {
            handleSubs(sub, (connection, subscription) -> {
                connection.subscriptions.emitNext(subscription, Reactors.emitFailureHandler());
            });
            return Mono.empty();
        }

        @Override
        public Mono<Void> unsub(ByteBuf sub) {
            handleSubs(sub, (connection, subscription) -> {
                connection.subscriptions.emitNext(subscription, Reactors.emitFailureHandler());
            });
            return Mono.empty();
        }

        @Override
        @SuppressWarnings("all")
        public Mono<Void> pub(ByteBuf buf) {
            try (ObjectInput input = createInput(buf)) {
                String serviceId = input.readUTF();
                RpcEventConnection connection = connections.get(serviceId);
                if (null != connection) {
                    String topic = input.readUTF();

                    Map<String, Object> headers = SerializeUtils.readMap(input, Maps::newLinkedHashMapWithExpectedSize);
                    Object payload = SerializeUtils.readObject(input);
                    TopicPayload topicPayload;
                    if (payload instanceof ByteBuf) {
                        topicPayload = TopicPayload.of(topic, Payload.of(((ByteBuf) payload)));
                    } else {
                        topicPayload = TopicPayload.of(topic, NativePayload.of(payload));
                    }
                    connection.producer.emitNext(topicPayload, Reactors.emitFailureHandler());
                }
            } catch (Throwable error) {
                log.error("Error handling subscription", error);
            }
            return Mono.empty();
        }

    }

    @Service
    public interface Api {
        @ServiceMethod
        Mono<Void> sub(ByteBuf subscription);

        @ServiceMethod
        Mono<Void> unsub(ByteBuf subscription);

        @ServiceMethod
        Mono<Void> pub(ByteBuf buf);

    }
}
