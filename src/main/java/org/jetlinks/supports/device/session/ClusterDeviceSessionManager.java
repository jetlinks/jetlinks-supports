package org.jetlinks.supports.device.session;

import com.google.common.collect.Maps;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.device.session.DeviceSessionInfo;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.MessageType;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.ServiceEvent;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.PersistentSession;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.core.utils.SerializeUtils;
import org.springframework.util.ObjectUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@Slf4j
public class ClusterDeviceSessionManager extends AbstractDeviceSessionManager {

    private final RpcManager rpcManager;

    private final Map<String, Service> services = new NonBlockingHashMap<>();

    /**
     * 集群会话同步间隔
     */
    @Getter
    @Setter
    private Duration syncInterval = Duration.ZERO;

    /**
     * 延迟集群会话同步
     */
    @Getter
    @Setter
    private Duration syncDelay = Duration.ofMinutes(1);


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
        Mono<Long> remove0(SessionOperation operation);

        @ServiceMethod
        Flux<DeviceSessionInfo> sessions();

        @ServiceMethod
        Mono<Void> sync(DeviceSessionInfo info);

        @Getter
        @Setter
        class SessionOperation implements Externalizable {
            private String deviceId;
            private Map<String, Object> context;

            public static SessionOperation of(String deviceId, ContextView ctx) {
                SessionOperation opt = new SessionOperation();
                opt.deviceId = deviceId;
                return opt.with(ctx);
            }

            @Override
            public void writeExternal(ObjectOutput out) throws IOException {
                out.writeUTF(deviceId);
                SerializeUtils.writeKeyValue(context, out);
            }

            @Override
            public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
                deviceId = in.readUTF();
                context = SerializeUtils.readMap(in, Maps::newHashMapWithExpectedSize);
            }

            public Context toContext() {
                if (context == null) {
                    return Context.empty();
                }
                Object msg = context.get("@msg");
                if (msg instanceof DeviceMessage) {
                    return Context.of(DeviceMessage.class, msg);
                } else if (msg instanceof Map) {
                    return MessageType
                        .<DeviceMessage>convertMessage(((Map<String, Object>) msg))
                        .map(_msg -> Context.of(DeviceMessage.class, _msg))
                        .orElse(Context.empty());
                }
                return Context.empty();
            }

            public SessionOperation with(ContextView view) {
                if (context == null) {
                    context = Maps.newHashMapWithExpectedSize(1);
                }
                view.getOrEmpty(DeviceMessage.class)
                    .ifPresent(msg -> context.put("@msg", msg));
                return this;
            }
        }
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
        public Mono<Long> remove0(SessionOperation operation) {
            return this
                .doWith(operation.deviceId,
                        AbstractDeviceSessionManager::removeFromCluster,
                        Reactors.ALWAYS_ZERO_LONG)
                .contextWrite(operation.toContext());
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

        @Override
        public Mono<Void> sync(DeviceSessionInfo info) {
            return doWith(info, (manager, session) -> {
                DeviceSessionRef ref = manager.localSessions.get(session.getDeviceId());
                if (ref != null) {
                    DeviceSession loaded = ref.loaded;
                    if (loaded != null) {
                        loaded.keepAlive(Math.max(session.getLastCommTime(), loaded.lastPingTime()));
                    }
                }
                return Mono.empty();
            }, Mono.empty());
        }
    }

    @Override
    public void init() {
        super.init();

        disposable.add(
            this.rpcManager.registerService(new ServiceImpl(() -> this))
        );
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

        // 定时同步会话信息到其他集群节点
        if (!syncInterval.isZero() && !syncInterval.isNegative()) {
            disposable.add(
                Flux.interval(syncDelay, syncInterval)
                    .onBackpressureDrop()
                    .concatMap(times -> doSync()
                        .onErrorResume(err -> {
                            log.warn("interval sync device session failed", err);
                            return Mono.empty();
                        }))
                    .subscribe()
            );
        }


    }

    private Mono<Void> doSync(DeviceSessionRef session) {
        DeviceSession loaded = session.loaded;
        if (loaded == null) {
            return Mono.empty();
        }
        DeviceSessionInfo info = DeviceSessionInfo.of(rpcManager.currentServerId(), loaded);
        log.debug("sync device session {} {}", info.getDeviceId(), loaded);
        return getServices()
            .flatMap(service -> service.sync(info))
            .then();
    }

    protected Mono<Void> doSync() {
        log.info("start sync device sessions");
        AtomicLong cnt = new AtomicLong();
        return Flux
            .fromIterable(localSessions.values())
            .filter(ref -> ref instanceof ClusterDeviceSessionRef && ((ClusterDeviceSessionRef) ref).needSync())
            .flatMap(ref -> {
                cnt.incrementAndGet();
                return this
                    .doSync(ref)
                    .onErrorResume(err -> {
                        log.warn("sync device {} session failed ", ref.deviceId, err);
                        return Mono.empty();
                    });
            }, 16)
            .then(Mono.fromRunnable(() -> {
                log.info("sync device sessions complete. sessions:{}", cnt);
            }));
    }

    @Override
    protected ClusterDeviceSessionRef newDeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, Mono<DeviceSession> ref) {
        return new ClusterDeviceSessionRef(deviceId, manager, ref);
    }

    @Override
    protected ClusterDeviceSessionRef newDeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, DeviceSession ref) {
        return new ClusterDeviceSessionRef(deviceId, manager, ref);
    }

    protected static class ClusterDeviceSessionRef extends DeviceSessionRef {
        static final AtomicLongFieldUpdater<ClusterDeviceSessionRef> LAST_SYNC =
            AtomicLongFieldUpdater.newUpdater(ClusterDeviceSessionRef.class, "lastSync");

        private volatile long lastSync;

        public ClusterDeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, Mono<DeviceSession> ref) {
            super(deviceId, manager, ref);
        }

        public ClusterDeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, DeviceSession ref) {
            super(deviceId, manager, ref);
        }

        public boolean needSync() {
            long lastTime = loaded.lastPingTime();
            // 持久化了并且上一次同步后变换了
            return loaded instanceof PersistentSession && LAST_SYNC.getAndSet(this, lastTime) != lastTime;
        }
    }

    @Override
    public boolean isShutdown() {
        return super.isShutdown() || rpcManager.isShutdown();
    }

    private void addService(String serverId, Service rpc) {
        services.put(serverId, new ErrorHandleService(serverId, rpc));
    }

    @AllArgsConstructor
    static class ErrorHandleService implements Service {
        private final String id;
        private final Service service;
        private static final Mono<Boolean> defaultAlive =
            Boolean.getBoolean("jetlinks.session.cluster.alive-when-failed") ?
                Reactors.ALWAYS_TRUE : Reactors.ALWAYS_FALSE;

        private void handleError(Throwable error) {
            log.warn("cluster[{}] session manager is failed", id, error);
        }

        @Override
        public Mono<Long> remove0(SessionOperation operation) {
            return service
                .remove0(operation)
                .onErrorResume(err -> {
                    handleError(err);
                    return Reactors.ALWAYS_ZERO_LONG;
                });
        }

        @Override
        public Mono<Boolean> isAlive(String deviceId) {
            return service
                .isAlive(deviceId)
                .onErrorResume(err -> {
                    handleError(err);
                    return defaultAlive;
                });
        }

        @Override
        public Mono<Boolean> checkAlive(String deviceId) {
            return service
                .checkAlive(deviceId)
                .onErrorResume(err -> {
                    handleError(err);
                    return defaultAlive;
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

        @Override
        public Mono<Void> sync(DeviceSessionInfo info) {
            return service
                .sync(info);
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
        // 传递上下文
        return Mono.deferContextual(ctx -> {
            Service.SessionOperation operation = Service.SessionOperation.of(deviceId, ctx);
            return this
                .getServices()
                .concatMap(service -> service.remove0(operation))
                .reduce(Math::addExact);
        });
//        return getServices()
//            .concatMap(service -> service.remove(deviceId))
//            .reduce(Math::addExact);
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
