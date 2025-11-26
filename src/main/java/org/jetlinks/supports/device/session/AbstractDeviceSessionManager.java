package org.jetlinks.supports.device.session;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.session.DeviceSessionEvent;
import org.jetlinks.core.device.session.DeviceSessionInfo;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.server.session.ChildrenDeviceSession;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public abstract class AbstractDeviceSessionManager implements DeviceSessionManager {

    private static final AtomicLongFieldUpdater<AbstractDeviceSessionManager>
        CLOSE_WIP = AtomicLongFieldUpdater.newUpdater(AbstractDeviceSessionManager.class, "closeWip");

    protected final Map<String, DeviceSessionRef> localSessions = new NonBlockingHashMap<>(2048);

    private final List<Function<DeviceSessionEvent, Mono<Void>>> sessionEventHandlers = new CopyOnWriteArrayList<>();

    protected final Disposable.Composite disposable = Disposables.composite();

    @Setter
    @Getter
    private Duration sessionLoadTimeout = Duration.ofSeconds(5);

    @Getter
    @Setter
    private Duration sessionCheckInterval = Duration.ofSeconds(30);

    @Getter
    @Setter
    private Duration sessionCheckDelay = Duration.ofMinutes(2);

    // 检查会话是否存活的并行度
    @Setter
    private int sessionCheckConcurrency = Integer.getInteger("jetlinks.session.check.concurrency",
                                                             Runtime.getRuntime().availableProcessors() * 64);

    @Setter
    private int sessionCloseConcurrency = Integer.getInteger("jetlinks.session.close.concurrency",
                                                             3000);

    public abstract String getCurrentServerId();

    protected abstract Mono<Boolean> initSessionConnection(DeviceSession session);

    protected abstract Mono<Long> removeRemoteSession(String deviceId);

    protected abstract Mono<Long> getRemoteTotalSessions();

    protected abstract Mono<Boolean> remoteSessionIsAlive(String deviceId);

    protected abstract Mono<Boolean> checkRemoteSessionIsAlive(String deviceId);

    protected abstract Flux<DeviceSessionInfo> remoteSessions(String serverId);

    protected abstract Flux<DeviceSessionInfo> remoteDeviceSessions(String deviceId);

    protected Sinks.Many<DeviceSession> closeSink = Reactors.createMany();
    private volatile long closeWip = 0;

    public void init() {
        Scheduler scheduler = Schedulers.newSingle("device-session-checker");
        disposable.add(scheduler);
        disposable.add(
            Flux.interval(sessionCheckDelay, sessionCheckInterval, scheduler)
                .onBackpressureDrop()
                .concatMap(time -> executeInterval())
                .subscribe()
        );

        disposable.add(
            closeSink
                .asFlux()
                .bufferTimeout(1000, Duration.ofSeconds(1))
                .onBackpressureBuffer()
                .concatMap(flux -> Flux
                    .fromIterable(flux)
                    .filter(session -> !localSessions.containsKey(session.getDeviceId()))
                    .flatMap(this::closeSessionSafe)
                    .then(), 0)
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

    public boolean isShutdown() {
        return disposable.isDisposed();
    }

    @Override
    public Mono<DeviceSession> getSession(String deviceId) {
        return getSession(deviceId, true);
    }

    @Override
    public Mono<DeviceSession> getSession(String deviceId,
                                          boolean unregisterWhenNotAlive) {
        if (ObjectUtils.isEmpty(deviceId)) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            DeviceSessionRef ref = localSessions.get(deviceId);
            if (ref == null || ref.isDisposed()) {
                return Mono.empty();
            }
            if (unregisterWhenNotAlive) {
                return ref.checkSessionAlive();
            }
            return ref.ref();
        });
    }

    @Override
    public Flux<DeviceSession> getSessions() {
        return Flux
            .fromIterable(localSessions.values())
            .flatMap(DeviceSessionRef::ref);
    }

    private Mono<DeviceSession> checkSessionAlive(String id) {
        DeviceSessionRef ref = localSessions.get(id);
        if (ref == null) {
            return Mono.empty();
        }
        return ref.checkSessionAlive();
    }

    @Override
    public final Mono<Long> remove(String deviceId, boolean onlyLocal) {
        if (onlyLocal) {
            return this.removeLocalSession(deviceId);
        } else {
            return Flux
                .concat(this.removeLocalSession(deviceId),
                        this.removeRemoteSession(deviceId))
                .reduce(Math::addExact);
        }
    }

    @Override
    public Mono<Long> remove(String deviceId, Predicate<DeviceSession> predicate) {
        DeviceSessionRef _ref = localSessions.get(deviceId);

        return _ref == null ? Reactors.ALWAYS_ZERO_LONG : _ref.close(predicate);
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
    public Mono<Boolean> checkAlive(String deviceId,
                                    boolean onlyLocal) {
        Mono<Boolean> localAlive = this.checkLocalAlive(deviceId);
        if (onlyLocal) {
            return localAlive;
        }
        return localAlive
            .flatMap(alive -> {
                if (alive) {
                    return Reactors.ALWAYS_TRUE;
                }
                return checkRemoteSessionIsAlive(deviceId);
            });
    }

    protected final Mono<Boolean> checkLocalAlive(String deviceId) {
        return this
            .getSession(deviceId)
            .flatMap(session -> session
                .getOperator() == null
                ? Reactors.ALWAYS_FALSE
                : syncConnectionInfo(session.getOperator(), session))
            .defaultIfEmpty(false);
    }

    protected final Mono<Boolean> syncConnectionInfo(DeviceOperator device, DeviceSession session) {
        return device
            .online(getCurrentServerId(),
                    session.getClientAddress().map(String::valueOf).orElse(""),
                    -1)
            .thenReturn(true);
//
//        return device
//                .getConnectionServerId()
//                .filter(getCurrentServerId()::equals)
//                //serverId为空或者不是当前服务器，则同步连接信息
//                .switchIfEmpty(Mono.defer(() -> device
//                        .online(getCurrentServerId(),
//                                session.getClientAddress().map(String::valueOf).orElse(""),
//                                -1)
//                        .then(Mono.empty())))
//                .thenReturn(true);

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
    public final Flux<DeviceSessionInfo> getSessionInfo() {
        return Flux.concat(
            getLocalSessionInfo(),
            remoteSessions(null));
    }

    @Override
    public Flux<DeviceSessionInfo> getDeviceSessionInfo(String deviceId) {
        if (StringUtils.hasText(deviceId)) {
            return Flux.concat(
                getLocalDeviceSessionInfo(deviceId),
                remoteDeviceSessions(deviceId));
        }
        return Flux.empty();
    }

    protected Flux<DeviceSessionInfo> getLocalDeviceSessionInfo(String deviceId) {
        DeviceSessionRef ref = localSessions.get(deviceId);
        if (ref == null) {
            return Flux.empty();
        }
        DeviceSession session = ref.loaded;
        if (session == null) {
            return Flux.empty();
        }
        return Flux.just(DeviceSessionInfo.of(getCurrentServerId(), session));
    }

    @Override
    public final Flux<DeviceSessionInfo> getSessionInfo(String serverId) {
        if (getCurrentServerId().equals(serverId)) {
            return getLocalSessionInfo();
        }
        return remoteSessions(serverId);
    }

    public final Flux<DeviceSessionInfo> getLocalSessionInfo() {
        return Flux
            .fromIterable(localSessions.values())
            .mapNotNull(ref -> ref.loaded)
            .map(session -> DeviceSessionInfo.of(getCurrentServerId(), session));
    }

    @Override
    public Mono<DeviceSession> compute(@Nonnull String deviceId,
                                       Mono<DeviceSession> creator,
                                       Function<DeviceSession, Mono<DeviceSession>> updater) {
        DeviceSessionRef ref = localSessions
            .compute(deviceId, (_id, old) -> {
                if (old == null) {
                    if (creator == null) {
                        return null;
                    }
                    //创建新会话
                    return newDeviceSessionRef(_id, this, creator);
                } else {
                    if (updater == null) {
                        return old;
                    }
                    //替换会话
                    old.update(s -> s.flatMap(updater));
                    return old;
                }
            });
        return ref == null ? Mono.empty() : ref.ref();
    }

    @Override
    public final Mono<DeviceSession> compute(@Nonnull String deviceId,
                                             @Nonnull Function<Mono<DeviceSession>, Mono<DeviceSession>> computer) {

        return localSessions
            .compute(deviceId,
                     (_id, old) -> {
                         if (old != null) {
                             old.update(computer);
                             return old;
                         } else {
                             return newDeviceSessionRef(_id, this, computer.apply(Mono.empty()));
                         }
                     })
            .ref();
    }

    private Mono<DeviceSession> handleSessionCompute0(DeviceSession old,
                                                      DeviceSession newSession) {
        //会话发生了变化,更新一下设备缓存的连接信息
        if (old != null
            && old.isChanged(newSession)
            && newSession.getOperator() != null) {
            log.info("device [{}] session [{}] changed to [{}]", old.getDeviceId(), old, newSession);
            //关闭旧会话
            old.close();
            return newSession
                .getOperator()
                .online(getCurrentServerId(),
                        newSession.getClientAddress().map(InetSocketAddress::toString).orElse(null),
                        -1)
                .then(handleSessionCompute(old, newSession));
        }
        return handleSessionCompute(old, newSession);
    }

    protected Mono<DeviceSession> handleSessionCompute(DeviceSession old,
                                                       DeviceSession newSession) {
        return Mono.just(newSession);
    }

    protected final Mono<Void> closeSessionSafe(DeviceSession session) {
        return closeSession0(session)
            .onErrorResume(err -> {
                log.warn("close session [{}] error", session.getDeviceId(), err);
                return Mono.empty();
            });
    }

    private Mono<Void> closeSession0(DeviceSession session) {
        long now = System.currentTimeMillis();
        try {
            session.close();
        } catch (Throwable ignore) {
        }
        if (session.getOperator() == null || isShutdown()) {
            CLOSE_WIP.decrementAndGet(this);
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
                    return fireEvent(DeviceSessionEvent.of(now, DeviceSessionEvent.Type.unregister, session, true));
                } else {
                    log.info("device [{}] session [{}] closed", session.getDeviceId(), session);
                    return session
                        .getOperator()
                        .offline()
                        .then(
                            fireEvent(DeviceSessionEvent.of(now, DeviceSessionEvent.Type.unregister, session, false))
                        );
                }
            })
            .doAfterTerminate(() -> CLOSE_WIP.decrementAndGet(this));
    }

    protected long getCloseWip() {
        return CLOSE_WIP.get(this);
    }

    protected final Mono<Void> closeSession(DeviceSession session) {
        //同时离线数量太大
        if (CLOSE_WIP.incrementAndGet(this) > sessionCloseConcurrency) {
            if (closeSink.tryEmitNext(session).isSuccess()) {
                return Mono.empty();
            }
        }
        return closeSession0(session);
    }

    protected final Mono<Long> removeLocalSession(String deviceId) {
        return Mono.deferContextual((ctx) -> {
            DeviceSessionRef inRef = ctx
                .<DeviceSessionRef>getOrEmpty(DeviceSessionRef.class)
                .orElse(null);
            DeviceSessionRef ref = localSessions.get(deviceId);
            if (ref != null) {
                //在compute时删除自己?
                if (inRef == ref) {
                    return Reactors.ALWAYS_ZERO_LONG;
                }
                return ref.close();
            }
            return Reactors.ALWAYS_ZERO_LONG;
        });
    }

    private Mono<DeviceSession> doRegister(DeviceSession session) {
        if (session.getOperator() == null) {
            return Mono.empty();
        }
        return this
            .remoteSessionIsAlive(session.getDeviceId())
            .flatMap(alive -> session
                .getOperator()
                .online(getCurrentServerId(),
                        session.getClientAddress().map(InetSocketAddress::toString).orElse(null),
                        alive ? -1 : session.connectTime())
                .then(fireEvent(DeviceSessionEvent.of(
                    session.connectTime(),
                    DeviceSessionEvent.Type.register,
                    session,
                    alive))))
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
        DeviceSessionRef ref = localSessions.get(deviceId);

        DeviceSession session;
        DeviceOperator device;
        if (ref != null && (session = ref.loaded) != null && (device = ref.loaded.getOperator()) != null) {
            return device
                .online(getCurrentServerId(),
                        session
                            .getClientAddress()
                            .map(String::valueOf).orElse(""),
                        -1)
                .thenReturn(true);
        }
        return Mono.empty();
    }

    protected Mono<Long> removeFromCluster(String deviceId) {
        DeviceSessionRef ref = localSessions.remove(deviceId);
        if (ref != null) {
            ref.dispose();
            DeviceSession session = ref.loaded;
            if (session != null) {
                session.close();
                if (session.getOperator() == null) {
                    return Reactors.ALWAYS_ONE_LONG;
                }
                long now = System.currentTimeMillis();
                return session
                    .getOperator()
                    .getConnectionServerId()
                    .map(getCurrentServerId()::equals)
                    .defaultIfEmpty(false)
                    .flatMap(sameServer -> {
                        //又上线了?
                        if (localSessions.containsKey(deviceId)) {
                            return Mono.empty();
                        }
                        Mono<Void> before = Mono.empty();
                        if (sameServer) {
                            //同一个服务
                            before = session.getOperator().offline().then();
                        }
                        return before
                            .then(this.fireEvent(DeviceSessionEvent.of(
                                now,
                                DeviceSessionEvent.Type.unregister,
                                session,
                                !sameServer
                            )));
                    })
                    .thenReturn(1L);
            }
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
            .filter(ref -> ref.loaded != null)
            .flatMap(ref -> ref
                         .checkSessionAlive()
                         .onErrorResume(err -> {
                             log.warn("check session alive error", err);
                             return Mono.empty();
                         }),
                     sessionCheckConcurrency)
            .then();
    }


    protected DeviceSessionRef newDeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, Mono<DeviceSession> ref) {
        return new DeviceSessionRef(deviceId, manager, ref);
    }

    protected DeviceSessionRef newDeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, DeviceSession ref) {
        return new DeviceSessionRef(deviceId, manager, ref);
    }

    protected static class DeviceSessionRef implements Disposable {
        @SuppressWarnings("rawtypes")
        private final static AtomicReferenceFieldUpdater<DeviceSessionRef, Mono> LOADER
            = AtomicReferenceFieldUpdater.newUpdater(DeviceSessionRef.class, Mono.class, "loader");
        protected volatile Mono<DeviceSession> loader;

        @SuppressWarnings("rawtypes")
        private final static AtomicReferenceFieldUpdater<DeviceSessionRef, Sinks.One> AWAIT
            = AtomicReferenceFieldUpdater.newUpdater(DeviceSessionRef.class, Sinks.One.class, "await");
        private volatile Sinks.One<DeviceSession> await;

        private final AbstractDeviceSessionManager manager;
        public final String deviceId;

        private final static AtomicReferenceFieldUpdater<DeviceSessionRef, DeviceSession> LOADED
            = AtomicReferenceFieldUpdater.newUpdater(DeviceSessionRef.class, DeviceSession.class, "loaded");
        public volatile DeviceSession loaded;

        private volatile Set<String> children;

        private final Disposable.Swap disposable = Disposables.swap();

        public Set<String> children() {
            if (children != null) {
                return children;
            }
            synchronized (this) {
                if (children != null) {
                    return children;
                }
                return children = ConcurrentHashMap.newKeySet();
            }
        }

        public void removeChild(String id) {
            if (children != null) {
                children.remove(id);
            }
        }

        public DeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, Mono<DeviceSession> ref) {
            this.deviceId = deviceId;
            this.manager = manager;
            this.update(o -> ref);
        }

        public DeviceSessionRef(String deviceId, AbstractDeviceSessionManager manager, DeviceSession ref) {
            this.deviceId = deviceId;
            this.manager = manager;
            this.loaded = ref;
        }

        private void handleParentChanged(DeviceSession from, DeviceSession to) {
            DeviceSessionRef fromRef = manager.localSessions.get(from.getDeviceId());
            DeviceSessionRef toRef = manager.localSessions.get(to.getDeviceId());
            if (null != fromRef) {
                fromRef.removeChild(deviceId);
            }
            if (null != toRef) {
                toRef.children().add(deviceId);
            }
        }

        private Mono<DeviceSession> handleLoaded(DeviceSession session) {
            if (isDisposed()) {
                DeviceSessionRef ref = manager.localSessions.get(deviceId);
                if (ref != null && ref != this) {
                    return ref.ref();
                }
                return Mono.just(session);
            }
            DeviceSession old = LOADED.getAndSet(this, session);

            handleParent(session, (s, parent) -> parent.children().add(s.getDeviceId()));

            if (session.isWrapFrom(ChildrenDeviceSession.class)) {
                session.unwrap(ChildrenDeviceSession.class)
                       .doOnParentChanged(this::handleParentChanged);
            }

            if (old == null) {
                return manager
                    .doRegister(session)
                    .then(manager.handleSessionCompute0(null, session));
            }
            return manager.handleSessionCompute0(old, session);
        }

        protected Mono<Long> close(Predicate<DeviceSession> test) {
            synchronized (this) {
                DeviceSession loaded = this.loaded;
                if (loaded != null && test.test(loaded)) {
                    return close();
                }
                return Reactors.ALWAYS_ZERO_LONG;
            }
        }

        private void afterLoaded(DeviceSession session) {
            DeviceSession loaded = this.loaded;
            if (loaded != null && !session.equals(loaded)) {
                loaded.close();
            }
            LOADED.set(this, session);
            @SuppressWarnings("all")
            Sinks.One<DeviceSession> await = AWAIT.getAndSet(this, null);
            if (await != null) {
                await.emitValue(session, Reactors.emitFailureHandler());
            }
        }

        protected void handleParent(DeviceSession session,
                                    BiConsumer<DeviceSession, DeviceSessionRef> parent) {
            if (session != null && session.isWrapFrom(ChildrenDeviceSession.class)) {
                DeviceSessionRef ref =
                    manager.localSessions
                        .get(session.unwrap(ChildrenDeviceSession.class).getParent().getDeviceId());

                if (null != ref) {
                    parent.accept(session, ref);
                }
            }
        }

        protected Mono<Void> checkChildren() {
            if (children != null) {
                return Flux
                    .fromIterable(children)
                    .flatMap(manager::checkSessionAlive)
                    .then();
            }
            return Mono.empty();
        }

        private Mono<Long> close() {
            try {
                if (isDisposed()) {
                    return Reactors.ALWAYS_ZERO_LONG;
                }
                boolean removed = manager.localSessions.remove(deviceId, this);
                if (removed) {
                    DeviceSession loaded = LOADED.getAndSet(this, null);
                    if (loaded != null) {
                        return doClose(loaded);
                    }
                }
                return Reactors.ALWAYS_ZERO_LONG;
            } finally {
                dispose();
            }
        }

        private Mono<Long> doClose(DeviceSession session) {
            //移除上级设备的子设备信息
            handleParent(session, (s, ref) -> ref.removeChild(s.getDeviceId()));
            return manager
                .closeSession(session)
                .then(checkChildren())
                .then(Reactors.ALWAYS_ONE_LONG);
        }

        private void loadError(Throwable err) {
            if (this.loaded != null) {
                //已经加载了会话，但是初始化失败了?
                this.loaded.close();
            }
            manager.localSessions.remove(deviceId, this);
            @SuppressWarnings("all")
            Sinks.One<DeviceSession> await = AWAIT.getAndSet(this, null);
            if (await != null) {
                await.emitError(err, Reactors.emitFailureHandler());
            } else {
                log.error("load device [{}] session error", deviceId, err);
            }
        }

        private void loadEmpty() {
            if (this.loaded != null) {
                this.loaded.close();
            }
            manager.localSessions.remove(deviceId, this);
            @SuppressWarnings("all")
            Sinks.One<DeviceSession> await = AWAIT.getAndSet(this, null);
            if (await != null) {
                await.emitEmpty(Reactors.emitFailureHandler());
            }
            dispose();
        }

        public void update(Function<Mono<DeviceSession>, Mono<DeviceSession>> updater) {
            synchronized (this) {
                @SuppressWarnings("all")
                Mono<DeviceSession> loader = LOADER.getAndSet(this, null);
                if (loader != null) {
                    loader = updater.apply(loader);
                } else {
                    @SuppressWarnings("all")
                    Sinks.One<DeviceSession> await = AWAIT.get(this);
                    if (await != null) {
                        loader = updater.apply(await.asMono());
                    } else {
                        loader = updater.apply(Mono.fromSupplier(() -> this.loaded));
                    }
                }
                AWAIT.compareAndSet(this, null, Sinks.one());
                LOADER.set(this, loader);
            }

        }

        @SuppressWarnings("all")
        private Mono<DeviceSession> tryLoad(ContextView ctx) {
            Mono<DeviceSession> loader = LOADER.getAndSet(this, null);
            //直接load
            if (loader != null) {
                Sinks.One<DeviceSession> async = Sinks.one();
                loader
                    .flatMap(this::handleLoaded)
                    .switchIfEmpty(Mono.fromRunnable(this::loadEmpty))
                    .timeout(manager.sessionLoadTimeout,
                             Mono.error(() -> new TimeoutException("device [" + deviceId + "] session load timeout")))
                    .subscribe(
                        loaded -> {
                            afterLoaded(loaded);
                            async.emitValue(LOADED.get(this), Reactors.emitFailureHandler());
                        },
                        err -> {
                            loadError(err);
                            async.emitError(err, Reactors.emitFailureHandler());
                        },
                        () -> {
                            async.emitEmpty(Reactors.emitFailureHandler());
                        },
                        Context.of(ctx).put(DeviceSessionRef.class, this)
                    );
                return async.asMono();
            }
            Sinks.One<DeviceSession> sink = AWAIT.get(this);
            if (sink == null) {
                return Mono.fromSupplier(() -> loaded);
            }
            //等待其他地方load
            return sink.asMono();
        }

        public Mono<DeviceSession> checkSessionAlive() {
            if (isDisposed()) {
                return Mono.empty();
            }
            DeviceSession session = this.loaded;
            if (session != null) {
                return session
                    .isAliveAsync()
                    .flatMap(alive -> {
                        if (!alive) {
                            return close().then(Mono.empty());
                        }
                        return Mono.just(session);
                    });
            }
            return ref().flatMap(ignore -> checkSessionAlive());
        }

        public Mono<DeviceSession> ref() {
            return Mono.deferContextual(ctx -> {
                //避免递归调用
                if (ctx.getOrEmpty(DeviceSessionRef.class).orElse(null) == this) {
                    return Mono.fromSupplier(() -> {
                        DeviceSession loaded = this.loaded;
                        if (loaded == null) {
                            log.warn("recursive call get device session [{}]", deviceId);
                        }
                        return loaded;
                    });
                }
                return tryLoad(ctx);
            });
        }

        @Override
        public void dispose() {
            @SuppressWarnings("all")
            Sinks.One<DeviceSession> await = AWAIT.getAndSet(this, null);
            if (await != null) {
                await.emitEmpty(Reactors.emitFailureHandler());
            }
            disposable.dispose();
        }

        @Override
        public boolean isDisposed() {
            return disposable.isDisposed();
        }
    }
}
