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

/**
 * 抽象设备会话管理器
 *
 * @author zhouhao
 */
@Slf4j
public abstract class AbstractDeviceSessionManager implements DeviceSessionManager {

    /**
     * 本地设备会话
     */
    protected final Map<String, Mono<DeviceSession>> localSessions = new ConcurrentHashMap<>();

    /**
     * 设备会话处理
     * <p>{@link CopyOnWriteArrayList} 保证并发下时的线程安全</p>
     */
    private final List<Function<DeviceSessionEvent, Mono<Void>>> sessionEventHandlers = new CopyOnWriteArrayList<>();

    /**
     * 多{@link Disposable}的容器，便于统一处置资源或任务
     */
    protected final Disposable.Composite disposable = Disposables.composite();

    /**
     * 当前集群节点ID
     *
     * @return 集群节点ID
     */
    public abstract String getCurrentServerId();

    /**
     * 初始化设备会话连接
     *
     * @param session 设备会话
     * @return
     */
    protected abstract Mono<Boolean> initSessionConnection(DeviceSession session);

    /**
     * 移除远程会话
     *
     * @param deviceId 设备ID
     * @return
     */
    protected abstract Mono<Long> removeRemoteSession(String deviceId);

    /**
     * 获取远程会话总数
     *
     * @return 远程会话总数
     */
    protected abstract Mono<Long> getRemoteTotalSessions();

    /**
     * 监测远程会话是否存活
     *
     * @param deviceId 设备ID
     * @return 是否存活
     */
    protected abstract Mono<Boolean> remoteSessionIsAlive(String deviceId);

    /**
     * 初始化会话管理器任务（定时检查设备状态）
     */
    public void init() {
        Scheduler scheduler = Schedulers.newSingle("device-session-checker");
        disposable.add(scheduler);
        disposable.add(
                // 每隔三十秒检查当前所有会话的状态
                Flux.interval(Duration.ofSeconds(30), scheduler)
                    .concatMap(time -> this
                            .checkSession()
                            .onErrorResume(err -> Mono.empty()))
                    .subscribe()
        );
    }

    /**
     * 结束所有任务
     */
    public void shutdown() {
        disposable.dispose();
    }

    /**
     * 获取设备会话.会话不存在则返回{@link  Mono#empty()}.
     * <p>
     * 此方法仅会返回本地存活的会话信息.
     *
     * @param deviceId 设备ID
     * @return 会话
     */
    @Override
    public Mono<DeviceSession> getSession(String deviceId) {
        return localSessions
                .getOrDefault(deviceId, Mono.empty())
                .filterWhen(this::checkSessionAlive);
    }

    /**
     * 检查设备会话是否存活
     *
     * @param session 设备会话
     * @return 是否存活
     */
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

    /**
     * 移除会话,如果会话存在将触发{@link DeviceSessionEvent}
     * <p>
     * 当设置参数{@code onlyLocal}为true时,将移除整个集群的会话.
     *
     * @param deviceId  设备ID
     * @param onlyLocal 是否只移除本地的会话信息
     * @return 有多少会话被移除
     */
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

    /**
     * 判断会话是否存活
     *
     * @param deviceId  设备ID
     * @param onlyLocal 是否仅判断本地的会话
     * @return 是否存活
     */
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

    /**
     * 获取会话总数
     *
     * @param onlyLocal 是否仅获取本地的会话数量
     * @return 总数
     */
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

    /**
     * 计算设备会话,通常用于注册和变更会话信息.
     * <p>
     * 如果之前会话不存在,执行后返回了新的会话,则将触发{@link  DeviceSessionEvent}.
     *
     * <pre>{@code
     *
     *  manager.compute(deviceId,old -> {
     *
     *   return old
     *          //会话已存在则替换为新的会话
     *          .map(this::replaceSession)
     *          //会话不存在则创建新的会话
     *          .switchIfEmpty(createNewSession());
     *  })
     *
     * }</pre>
     *
     * @param deviceId 设备ID
     * @param computer 会话处理器
     * @return 处理后的会话
     * @see DeviceSessionEvent.Type#register
     */
    @Override
    public Mono<DeviceSession> compute(String deviceId,
                                       Function<Mono<DeviceSession>, Mono<DeviceSession>> computer) {

        return localSessions
                .compute(deviceId,
                         (_id, old) -> {
                             if (old == null) {
                                 old = Mono.empty();
                             }
                             // 旧会话替换成新会话
                             // 新会话为空则移除旧会话
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

    /**
     * 根据规则计算旧设备会话
     * <li>旧会话不存在就注册新会话</li>
     * <li>旧会话存在就替换为新会话</li>
     *
     * @param oldSession 旧设备会话
     * @param computer   计算规则
     * @return 设备会话
     */
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

    /**
     * 新会话替换旧会话
     *
     * @param old        旧会话
     * @param newSession 新会话
     * @return 新会话
     */
    protected Mono<DeviceSession> handleSessionCompute(DeviceSession old,
                                                       DeviceSession newSession) {
        return Mono.just(newSession);
    }

    /**
     * 移除设备的本地会话
     *
     * @param deviceId 设备ID
     * @return 移除数量统计
     */
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

    /**
     * 设备注册
     *
     * @param session 设备会话
     * @return 设备会话
     */
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

    /**
     * 设备会话事件处理
     *
     * @param event 设备会话事件
     * @return Void
     */
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

    /**
     * 设备上线初始化操作
     *
     * @param deviceId 设备ID
     * @return 初始化成功
     */
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

    /**
     * 从集群中移除设备会话
     *
     * @param deviceId 设备ID
     * @return 被移除的数量
     */
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

    /**
     * 监听并处理会话事件,可通过调用返回值{@link  Disposable#dispose()}来取消监听
     *
     * @param handler 事件处理器
     * @return Disposable
     */
    @Override
    public Disposable listenEvent(Function<DeviceSessionEvent, Mono<Void>> handler) {
        sessionEventHandlers.add(handler);
        return () -> sessionEventHandlers.remove(handler);
    }

    /**
     * 检查本地设备会话
     *
     * @return Void
     */
    protected Mono<Void> checkSession() {
        return Flux
                .fromIterable(localSessions.values())
                .flatMap(Function.identity())
                .concatMap(this::checkSessionAlive)
                .then();
    }
}
