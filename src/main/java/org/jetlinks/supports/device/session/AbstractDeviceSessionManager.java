package org.jetlinks.supports.device.session;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.session.DeviceSessionEvent;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

@Slf4j
public abstract class AbstractDeviceSessionManager implements DeviceSessionManager {

    protected final Map<String, Mono<DeviceSession>> localSessions = new ConcurrentHashMap<>();

    private final List<Function<DeviceSessionEvent, Mono<Void>>> sessionEventHandlers = new CopyOnWriteArrayList<>();

    protected final Disposable.Composite disposable = Disposables.composite();

    public abstract String getCurrentServerId();

    protected abstract Mono<Boolean> initSessionConnection(DeviceSession session);

    protected abstract Mono<Long> removeRemoteSession(String deviceId);

    protected abstract Mono<Long> getRemoteTotalSessions();

    protected abstract Mono<Boolean> remoteSessionIsAlive(String deviceId);

    public void init() {
        Scheduler scheduler = Schedulers.newSingle("device-session-checker");
        disposable.add(scheduler);
        disposable.add(
                Flux.interval(Duration.ofSeconds(30), scheduler)
                    .concatMap(time -> executeInterval())
                    .subscribe()
        );
    }

    protected Mono<Void> executeInterval() {
        return this
                .checkSession()
                .onErrorResume(err -> Mono.empty());
    }

    public void shutdown() {
        disposable.dispose();
    }

    @Override
    public Mono<DeviceSession> getSession(String deviceId) {
        return localSessions
                .getOrDefault(deviceId, Mono.empty())
                .filterWhen(this::checkSessionAlive);
    }

    private Mono<Boolean> checkSessionAlive(DeviceSession session) {
        if (!session.isAlive()) {
            //尝试重新初始化设备会话连接信息
            return this
                    .initSessionConnection(session)
                    .flatMap(success -> {
                        //会话在其他节点依旧存活
                        if (success) {
                            return Reactors.ALWAYS_FALSE;
                        }
                        return this
                                .removeLocalSession(session.getDeviceId())
                                .thenReturn(false);
                    });
        }
        return Reactors.ALWAYS_TRUE;
    }

    @Override
    public final Mono<Long> remove(String deviceId, boolean onlyLocal) {
        if (onlyLocal) {
            return this.removeLocalSession(deviceId);
        } else {
            return Flux
                    .merge(this.removeLocalSession(deviceId),
                           this.removeRemoteSession(deviceId))
                    .reduce(Math::addExact);
        }
    }

    @Override
    public final Mono<Boolean> isAlive(String deviceId, boolean onlyLocal) {
        Mono<Boolean> localAlive = this
                .getSession(deviceId)
                .hasElement();
        if (onlyLocal) {
            return localAlive;
        }
        return localAlive
                .flatMap(alive -> {
                    if (alive) {
                        return Reactors.ALWAYS_TRUE;
                    }
                    return remoteSessionIsAlive(deviceId);
                });
    }

    @Override
    public final Mono<Long> totalSessions(boolean onlyLocal) {
        Mono<Long> total = Mono.just((long) localSessions.size());
        if (onlyLocal) {
            return total;
        }
        return Mono
                .zip(total,
                     getRemoteTotalSessions(),
                     Math::addExact);
    }

    @Override
    public Mono<DeviceSession> compute(String deviceId,
                                       Function<Mono<DeviceSession>, Mono<DeviceSession>> computer) {

        return localSessions
                .compute(deviceId,
                         (_id, old) -> {
                             if (old == null) {
                                 old = Mono.empty();
                             }
                             return old
                                     .map(oldSession -> doCompute(oldSession, computer))
                                     .defaultIfEmpty(Mono.defer(() -> doCompute(null, computer)))
                                     .flatMap(Function.identity())
                                     .switchIfEmpty(Mono.fromRunnable(() -> localSessions.remove(deviceId)))
                                     .cache();
                         })
                .doOnNext(session -> localSessions.put(deviceId, Mono.just(session)))
                .doOnError(err -> localSessions.remove(deviceId));
    }

    private Mono<DeviceSession> doCompute(DeviceSession oldSession, Function<Mono<DeviceSession>, Mono<DeviceSession>> computer) {
        return computer
                .apply(oldSession == null ? Mono.empty() : Mono.just(oldSession))
                .flatMap(newSession -> {
                    if (null == oldSession) {
                        return this.doRegister(newSession);
                    }
                    return handleSessionCompute(oldSession, newSession);
                });
    }

    protected Mono<DeviceSession> handleSessionCompute(DeviceSession old,
                                                       DeviceSession newSession) {
        return Mono.just(newSession);
    }

    protected final Mono<Long> removeLocalSession(String deviceId) {
        Mono<DeviceSession> sessionMono = localSessions.remove(deviceId);
        if (sessionMono != null) {
            return sessionMono
                    .flatMap(session -> {
                        session.close();
                        if (session.getOperator() == null) {
                            return Reactors.ALWAYS_ONE_LONG;
                        }
                        return this
                                .initSessionConnection(session)
                                .flatMap(alive -> {
                                    if (!alive) {
                                        return session
                                                .getOperator()
                                                .offline()
                                                .then(
                                                        fireEvent(DeviceSessionEvent.of(DeviceSessionEvent.Type.unregister, session, false))
                                                );
                                    }
                                    return fireEvent(DeviceSessionEvent.of(DeviceSessionEvent.Type.unregister, session, true));
                                })
                                .thenReturn(1L);
                    });
        }
        return Reactors.ALWAYS_ZERO_LONG;
    }

    private Mono<DeviceSession> doRegister(DeviceSession session) {
        if (session.getOperator() == null) {
            return Mono.empty();
        }
        return this
                .remoteSessionIsAlive(session.getDeviceId())
                .flatMap(alive -> session
                        .getOperator()
                        .online(getCurrentServerId(), session.getId(), session
                                .getClientAddress()
                                .map(InetSocketAddress::toString)
                                .orElse(null))
                        .then(fireEvent(DeviceSessionEvent.of(DeviceSessionEvent.Type.register, session, alive))))
                .thenReturn(session);
    }

    protected Mono<Void> fireEvent(DeviceSessionEvent event) {
        if (sessionEventHandlers.isEmpty()) {
            return Mono.empty();
        }
        return Flux
                .fromIterable(sessionEventHandlers)
                .flatMap(handler -> Mono
                        .defer(() -> handler.apply(event))
                        .onErrorResume(err -> {
                            log.error("fire session event error {}", event, err);
                            return Mono.empty();
                        }))
                .then();
    }

    protected Mono<Boolean> doInit(String deviceId) {
        return localSessions
                .getOrDefault(deviceId, Mono.empty())
                .flatMap(session -> session.getOperator() == null
                        ? Reactors.ALWAYS_FALSE
                        : session
                        .getOperator()
                        .online(getCurrentServerId(), null)
                        .thenReturn(true))
                .defaultIfEmpty(false);
    }

    protected Mono<Long> removeFromCluster(String deviceId) {
        Mono<DeviceSession> sessionMono = localSessions.remove(deviceId);
        if (sessionMono != null) {
            return sessionMono
                    .flatMap(session -> {
                        session.close();
                        if (session.getOperator() == null) {
                            return Reactors.ALWAYS_ONE_LONG;
                        }
                        return session
                                .getOperator()
                                .getConnectionServerId()
                                .map(getCurrentServerId()::equals)
                                .defaultIfEmpty(false)
                                .flatMap(sameServer -> {
                                    Mono<Void> before = Mono.empty();
                                    if (sameServer) {
                                        //同一个服务
                                        before = session.getOperator().offline().then();
                                    }
                                    return before
                                            .then(this.fireEvent(DeviceSessionEvent.of(
                                                    DeviceSessionEvent.Type.unregister,
                                                    session,
                                                    !sameServer
                                            )));
                                })
                                .thenReturn(1L);
                    });
        }
        return Reactors.ALWAYS_ZERO_LONG;
    }

    @Override
    public Disposable listenEvent(Function<DeviceSessionEvent, Mono<Void>> handler) {
        sessionEventHandlers.add(handler);
        return () -> sessionEventHandlers.remove(handler);
    }

    protected Mono<Void> checkSession() {
        return Flux
                .fromIterable(localSessions.values())
                .flatMap(Function.identity())
                .concatMap(this::checkSessionAlive)
                .then();
    }
}
