package org.jetlinks.supports.device.session;

import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.supports.scalecube.EmptyServiceMethodRegistry;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@Slf4j
@Deprecated
public class MicroserviceDeviceSessionManager extends AbstractDeviceSessionManager {

    private final Map<String, Service> services = new ConcurrentHashMap<>();

    private static final String SERVER_ID_TAG = "msid";

    private static final String SERVER_HELLO = "session-manager-hello";

    private final ExtendedCluster cluster;

    private final ServiceCall serviceCall;

    public MicroserviceDeviceSessionManager(ExtendedCluster cluster,
                                            ServiceCall serviceCall) {
        this.cluster = cluster;
        this.serviceCall = serviceCall;
        this.cluster.handler(ignore -> new ServiceHandler());
        sayHello();
    }

    @io.scalecube.services.annotations.Service
    public interface Service {
        @ServiceMethod
        Mono<Boolean> isAlive(String deviceId);

        @ServiceMethod
        Mono<Long> total();

        @ServiceMethod
        Mono<Boolean> init(String deviceId);

        @ServiceMethod
        Mono<Long> remove(String deviceId);
    }

    @AllArgsConstructor
    public static class ServiceImpl implements Service {
        private final Supplier<AbstractDeviceSessionManager> managerSupplier;

        private <T, Arg0> T doWith(Arg0 arg0,
                                   BiFunction<AbstractDeviceSessionManager, Arg0, T> arg,
                                   T defaultValue) {
            AbstractDeviceSessionManager manager = managerSupplier.get();
            if (manager == null) {
                return defaultValue;
            }
            return arg.apply(manager, arg0);
        }

        @Override
        public Mono<Boolean> isAlive(String deviceId) {
            return doWith(deviceId,
                          (manager, id) -> manager.isAlive(id, true),
                          Reactors.ALWAYS_FALSE);
        }

        @Override
        public Mono<Long> total() {
            return doWith(null,
                          (manager, nil) -> manager.totalSessions(true),
                          Reactors.ALWAYS_ZERO_LONG);
        }

        @Override
        public Mono<Boolean> init(String deviceId) {
            return doWith(deviceId,
                          AbstractDeviceSessionManager::doInit,
                          Reactors.ALWAYS_FALSE);
        }

        @Override
        public Mono<Long> remove(String deviceId) {
            return doWith(deviceId,
                          AbstractDeviceSessionManager::removeFromCluster,
                          Reactors.ALWAYS_ZERO_LONG);
        }
    }

    @Override
    public void init() {
        super.init();

        disposable.add(
                Flux.interval(Duration.ofSeconds(30))
                    .doOnNext(ignore -> this.sayHello())
                    .subscribe()
        );
    }

    private void sayHello() {
        cluster.spreadGossip(Message
                                     .builder()
                                     .qualifier(SERVER_HELLO)
                                     .data(getCurrentServerId())
                                     .build())
               .subscribe();
    }

    class ServiceHandler implements ClusterMessageHandler {
        @Override
        public void onGossip(Message gossip) {
            if (Objects.equals(SERVER_HELLO, gossip.qualifier())) {
                String id = gossip.data();
                services.put(id, createService(id));
            }
        }

        @Override
        public void onMessage(Message message) {

        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            if (event.isRemoved() || event.isLeaving()) {
                Member member = event.member();
                services.remove(member.id());
                if (StringUtils.hasText(member.alias())) {
                    services.remove(member.alias());
                }
            }
        }
    }

    @AllArgsConstructor
    static class ErrorHandleService implements Service {
        private final String id;
        private final Service service;

        private void handleError(Throwable error) {
            log.warn("cluster[{}] session manager is failed", id, error);
        }

        @Override
        public Mono<Boolean> isAlive(String deviceId) {
            return service
                    .isAlive(deviceId)
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_FALSE;
                    });
        }

        @Override
        public Mono<Long> total() {
            return service
                    .total()
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_ZERO_LONG;
                    });
        }

        @Override
        public Mono<Boolean> init(String deviceId) {
            return service
                    .init(deviceId)
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_FALSE;
                    });
        }

        @Override
        public Mono<Long> remove(String deviceId) {
            return service
                    .remove(deviceId)
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_ZERO_LONG;
                    });
        }
    }

    private Service createService(String id) {
        Service remote = serviceCall
                .router((serviceRegistry, request) -> {
                    if (serviceRegistry == null) {
                        return Optional.empty();
                    }
                    for (ServiceReference serviceReference : serviceRegistry.lookupService(request)) {
                        if (Objects.equals(id, serviceReference.tags().get(SERVER_ID_TAG))) {
                            return Optional.of(serviceReference);
                        }
                    }
                    return Optional.empty();
                })
                .methodRegistry(EmptyServiceMethodRegistry.INSTANCE)
                .api(Service.class);
        return new ErrorHandleService(id, remote);
    }


    public static ServiceInfo createService(String serverId,
                                            Supplier<AbstractDeviceSessionManager> managerSupplier) {
        return ServiceInfo
                .fromServiceInstance(new ServiceImpl(managerSupplier))
                .tag(SERVER_ID_TAG, serverId)
                .build();
    }

    @Override
    public final String getCurrentServerId() {
        String id = cluster.member().alias();

        return id == null ? cluster.member().id() : id;
    }

    @Override
    protected final Mono<Boolean> initSessionConnection(DeviceSession session) {
        if (services.size() == 0) {
            return Reactors.ALWAYS_FALSE;
        }
        return getServices()
                .concatMap(service -> service.init(session.getDeviceId()))
                .takeUntil(Boolean::booleanValue)
                .any(Boolean::booleanValue);
    }

    @Override
    protected final Mono<Long> removeRemoteSession(String deviceId) {
        if (services.size() == 0) {
            return Reactors.ALWAYS_ZERO_LONG;
        }
        return getServices()
                .flatMap(service -> service.remove(deviceId))
                .reduce(Math::addExact);
    }

    @Override
    protected final Mono<Long> getRemoteTotalSessions() {
        if (services.size() == 0) {
            return Reactors.ALWAYS_ZERO_LONG;
        }
        return this
                .getServices()
                .flatMap(Service::total)
                .reduce(Math::addExact);
    }

    @Override
    protected final Mono<Boolean> remoteSessionIsAlive(String deviceId) {
        if (services.size() == 0) {
            return Reactors.ALWAYS_FALSE;
        }
        return getServices()
                .flatMap(service -> service.isAlive(deviceId))
                .any(Boolean::booleanValue)
                .defaultIfEmpty(false);
    }

    private Flux<Service> getServices() {
        return Flux.fromIterable(services.values());
    }
}
