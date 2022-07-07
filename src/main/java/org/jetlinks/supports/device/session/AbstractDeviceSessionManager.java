package org.jetlinks.supports.device.session;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.device.session.DeviceSessionEvent;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

@Slf4j
public abstract class AbstractDeviceSessionManager implements DeviceSessionManager {

    protected final Map<String, Mono<DeviceSession>> localSessions = new NonBlockingHashMap<>();

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
        return getSession(deviceId, true);
    }

    @Override
    public Mono<DeviceSession> getSession(String deviceId,
                                          boolean unregisterWhenNotAlive) {
        if (StringUtils.isEmpty(deviceId)) {
            return Mono.empty();
        }
        if (unregisterWhenNotAlive) {
            return localSessions
                    .getOrDefault(deviceId, Mono.empty())
                    .filterWhen(this::checkSessionAlive);
        }
        return localSessions.getOrDefault(deviceId, Mono.empty());
    }

    @Override
    public Flux<DeviceSession> getSessions() {
        return Flux
                .fromIterable(localSessions.values())
                .flatMap(Function.identity());
    }

    private Mono<Boolean> checkSessionAlive(DeviceSession session) {
        return session
                .isAliveAsync()
                .defaultIfEmpty(true)
                .flatMap(alive -> {
                    if (!alive) {
                        //移除本地会话
                        return this
                                .removeLocalSession(session)
                                .thenReturn(false);
                    }
                    return Reactors.ALWAYS_TRUE;
                });
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
    public Mono<DeviceSession> compute(@Nonnull String deviceId,
                                       Mono<DeviceSession> creator,
                                       Function<DeviceSession, Mono<DeviceSession>> updater) {
        Mono<DeviceSession> ref = localSessions
                .compute(deviceId, (_id, old) -> {
                    Mono<DeviceSession> operator;
                    if (old == null) {
                        if (creator == null) {
                            return null;
                        }
                        //创建新会话
                        operator = creator
                                .flatMap(this::doRegister)
                                .doOnNext(this::replaceSession);
                    } else {
                        if (updater == null) {
                            return old;
                        }
                        //替换会话
                        operator = old
                                .flatMap(session -> updater
                                        .apply(session)
                                        .flatMap(newSession -> {

                                            replaceSession(newSession);

                                            return handleSessionCompute0(session, newSession);
                                        })
                                        .switchIfEmpty(Mono.defer(() -> removeLocalSession(deviceId).then(Mono.empty()))));
                    }
                    //cache
                    return operator
                            .doOnError(err -> localSessions.remove(deviceId))
                            .cache();
                });
        return ref == null ? Mono.empty() : ref;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private void replaceSession(DeviceSession newSession) {
        Mono<DeviceSession> old = localSessions.put(newSession.getDeviceId(), Mono.just(newSession));
        if (old instanceof Callable) {
            DeviceSession oldSession = ((Callable<DeviceSession>) old).call();
            if (null != oldSession && oldSession != newSession && oldSession.isChanged(newSession)) {
                log.info("close changed device [{}] session {} ,new session: {}", newSession.getDeviceId(), oldSession, newSession);
                oldSession.close();
            }
        }
    }

    @Override
    public final Mono<DeviceSession> compute(@Nonnull String deviceId,
                                             @Nonnull Function<Mono<DeviceSession>, Mono<DeviceSession>> computer) {

        return localSessions
                .compute(deviceId,
                         (_id, old) -> {
                             Mono<DeviceSession> oldMono = old == null ? Mono.empty() : old;
                             return oldMono
                                     .map(oldSession -> doCompute(oldSession, computer))
                                     .defaultIfEmpty(Mono.defer(() -> doCompute(null, computer)))
                                     .flatMap(Function.identity())
                                     //替换会话
                                     .doOnNext(this::replaceSession)
                                     //发生错误时关闭连接
                                     .onErrorResume(err -> {
                                         log.warn("compute device[{}] session error", deviceId, err);
                                         return removeAndClose(deviceId, oldMono).then(Mono.error(err));
                                     })
                                     //计算后返回空,移除本地会话
                                     .switchIfEmpty(Mono.defer(() -> removeAndClose(deviceId, oldMono).then(Mono.empty())))
                                     .cache();
                         });
    }

    private Mono<DeviceSession> removeAndClose(String deviceId, Mono<DeviceSession> mono) {
        localSessions.remove(deviceId);

        return mono
                .doOnNext(DeviceSession::close);

    }

    private Mono<DeviceSession> doCompute(DeviceSession oldSession, Function<Mono<DeviceSession>, Mono<DeviceSession>> computer) {
        return computer
                .apply(oldSession == null ? Mono.empty() : Mono.just(oldSession))
                //计算返回空，则认为会话注销
                .switchIfEmpty(oldSession == null ? Mono.empty() : closeSession(oldSession).then(Mono.empty()))
                .flatMap(newSession -> {
                    if (null == oldSession) {
                        return this.doRegister(newSession);
                    }
                    return handleSessionCompute0(oldSession, newSession);
                });
    }

    private Mono<DeviceSession> handleSessionCompute0(DeviceSession old,
                                                      DeviceSession newSession) {
        //会话发生了变化,更新一下设备缓存的连接信息
        if (old != null
                && old.isChanged(newSession)
                && newSession.getOperator() != null) {
            //关闭旧会话
            old.close();
            return newSession
                    .getOperator()
                    .online(getCurrentServerId(), newSession.getId(), newSession
                            .getClientAddress()
                            .map(InetSocketAddress::toString)
                            .orElse(null))
                    .then(
                            handleSessionCompute(old, newSession)
                    );
        }
        return handleSessionCompute(old, newSession);
    }

    protected Mono<DeviceSession> handleSessionCompute(DeviceSession old,
                                                       DeviceSession newSession) {
        return Mono.just(newSession);
    }

    protected final Mono<Void> closeSession(DeviceSession session) {
        try {
            session.close();
        } catch (Throwable ignore) {
        }
        if (session.getOperator() == null) {
            return Mono.empty();
        }
        return this
                //初始化会话连接信息,判断设备是否在其他服务节点还存在连接
                .initSessionConnection(session)
                .flatMap(alive -> {
                    //其他节点存活
                    //或者本地会话存在,可能在离线的同时又上线了?
                    //都认为设备会话依然存活
                    boolean sessionExists = alive || localSessions.containsKey(session.getDeviceId());
                    if (sessionExists) {
                        log.info("device [{}] session [{}] closed,but session still exists!", session.getDeviceId(), session);
                        return fireEvent(DeviceSessionEvent.of(DeviceSessionEvent.Type.unregister, session, true));
                    } else {
                        log.info("device [{}] session [{}] closed", session.getDeviceId(), session);
                        return session
                                .getOperator()
                                .offline()
                                .then(
                                        fireEvent(DeviceSessionEvent.of(DeviceSessionEvent.Type.unregister, session, false))
                                );
                    }

                });
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private Mono<Long> removeLocalSession(DeviceSession session) {
        Mono<DeviceSession> oldSessionMono = localSessions.get(session.getDeviceId());
        if (null == oldSessionMono) {
            return Reactors.ALWAYS_ZERO_LONG;
        }
        if (oldSessionMono instanceof Callable) {
            DeviceSession oldSession = ((Callable<DeviceSession>) oldSessionMono).call();
            //旧的会话与新的会话不同,则移除
            if (oldSession != null && oldSession == session) {
                if (localSessions.remove(session.getDeviceId(), oldSessionMono)) {
                    return closeSession(session)
                            .then(Reactors.ALWAYS_ONE_LONG);
                }
            }

        }
        return Reactors.ALWAYS_ZERO_LONG;

    }

    protected final Mono<Long> removeLocalSession(String deviceId) {
        Mono<DeviceSession> sessionMono = localSessions.remove(deviceId);
        if (sessionMono != null) {
            return sessionMono
                    .flatMap(session -> this
                            .closeSession(session)
                            .thenReturn(1L));
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
