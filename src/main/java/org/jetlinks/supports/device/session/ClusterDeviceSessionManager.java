package org.jetlinks.supports.device.session;

import io.scalecube.services.annotations.ServiceMethod;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.device.session.DeviceSessionInfo;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.ServiceEvent;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@Slf4j
public class ClusterDeviceSessionManager extends AbstractDeviceSessionManager {

    private final RpcManager rpcManager;

    private final Map<String, Service> services = new NonBlockingHashMap<>();

    public ClusterDeviceSessionManager(RpcManager rpcManager) {
        this.rpcManager = rpcManager;
    }

    @io.scalecube.services.annotations.Service
    public interface Service {
        @ServiceMethod
        Mono<Boolean> isAlive(String deviceId);

        @ServiceMethod
        Mono<Boolean> checkAlive(String deviceId);

        @ServiceMethod
        Mono<Long> total();

        @ServiceMethod
        Mono<Boolean> init(String deviceId);

        @ServiceMethod
        Mono<Long> remove(String deviceId);

        @ServiceMethod
        Flux<DeviceSessionInfo> sessions();
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
        public Mono<Boolean> checkAlive(String deviceId) {
            return doWith(deviceId,
                          (manager, id) -> manager.checkLocalAlive(deviceId),
                          Reactors.ALWAYS_FALSE);
        }

        @Override
        public Mono<Boolean> isAlive(String deviceId) {
            return doWith(deviceId,
                          (manager, id) -> {
                              DeviceSessionRef ref = manager.localSessions.get(deviceId);
                              if (ref == null) {
                                  return Reactors.ALWAYS_FALSE;
                              }
                              //加载中也认为存活
                              if (ref.loaded == null) {
                                  return Reactors.ALWAYS_TRUE;
                              }
                              return ref.loaded.isAliveAsync();
                          },
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

        @Override
        public Flux<DeviceSessionInfo> sessions() {
            return doWith(null,
                          (manager, ignore) -> manager.getLocalSessionInfo(),
                          Flux.empty());
        }
    }

    @Override
    public void init() {
        super.init();

        this.rpcManager.registerService(new ServiceImpl(() -> this));
        this.rpcManager
                .getServices(Service.class)
                .subscribe(service -> {
                    addService(service.serverNodeId(), service.service());
                });

        this.rpcManager
                .listen(Service.class)
                .subscribe(e -> {
                    if (e.getType() == ServiceEvent.Type.removed) {
                        services.remove(e.getServerNodeId());
                    } else if (e.getType() == ServiceEvent.Type.added) {
                        this.rpcManager
                                .getService(e.getServerNodeId(), Service.class)
                                .subscribe(service -> addService(e.getServerNodeId(), service));
                    }
                });
    }

    private void addService(String serverId, Service rpc) {
        services.put(serverId, new ErrorHandleService(serverId, rpc));
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
        public Mono<Boolean> checkAlive(String deviceId) {
            return service
                    .checkAlive(deviceId)
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

        @Override
        public Flux<DeviceSessionInfo> sessions() {
            return service
                    .sessions()
                    .onErrorResume(err -> {
                        handleError(err);
                        return Mono.empty();
                    });
        }
    }


    @Override
    public final String getCurrentServerId() {
        return rpcManager.currentServerId();
    }

    @Override
    protected final Mono<Boolean> initSessionConnection(DeviceSession session) {
        if (services.isEmpty()) {
            return Reactors.ALWAYS_FALSE;
        }
        return getServices()
                .concatMap(service -> service.init(session.getDeviceId()))
                .takeUntil(Boolean::booleanValue)
                .any(Boolean::booleanValue);
    }

    @Override
    protected final Mono<Long> removeRemoteSession(String deviceId) {
        if (services.isEmpty()) {
            return Reactors.ALWAYS_ZERO_LONG;
        }
        return getServices()
                .concatMap(service -> service.remove(deviceId))
                .reduce(Math::addExact);
    }

    @Override
    protected final Mono<Long> getRemoteTotalSessions() {
        if (services.isEmpty()) {
            return Reactors.ALWAYS_ZERO_LONG;
        }
        return this
                .getServices()
                .flatMap(Service::total)
                .reduce(Math::addExact);
    }

    @Override
    protected final Mono<Boolean> remoteSessionIsAlive(String deviceId) {
        if (services.isEmpty()) {
            return Reactors.ALWAYS_FALSE;
        }
        return getServices()
                .flatMap(service -> service.isAlive(deviceId))
                .any(Boolean::booleanValue)
                .defaultIfEmpty(false);
    }

    @Override
    protected Mono<Boolean> checkRemoteSessionIsAlive(String deviceId) {
        if (services.isEmpty()) {
            return Reactors.ALWAYS_FALSE;
        }
        return getServices()
                .flatMap(service -> service.checkAlive(deviceId))
                .any(Boolean::booleanValue)
                .defaultIfEmpty(false);
    }

    @Override
    protected Flux<DeviceSessionInfo> remoteSessions(String serverId) {
        if (ObjectUtils.isEmpty(serverId)) {
            return getServices()
                    .flatMap(Service::sessions);
        }
        Service service = services.get(serverId);
        return service == null ? Flux.empty() : service.sessions();
    }

    private Flux<Service> getServices() {
        return Flux.fromIterable(services.values());
    }
}
