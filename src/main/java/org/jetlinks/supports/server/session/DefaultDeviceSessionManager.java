package org.jetlinks.supports.server.session;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.monitor.GatewayServerMonitor;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.DeviceSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class DefaultDeviceSessionManager implements DeviceSessionManager {

    private final Map<String, DeviceSession> repository = new ConcurrentHashMap<>(4096);

    @Getter
    @Setter
    private Logger log = LoggerFactory.getLogger(DefaultDeviceSessionManager.class);

    @Getter
    @Setter
    private GatewayServerMonitor gatewayServerMonitor;

    @Getter
    @Setter
    private ScheduledExecutorService executorService;

    private EmitterProcessor<DeviceSession> onDeviceRegister = EmitterProcessor.create(false);

    private EmitterProcessor<DeviceSession> onDeviceUnRegister = EmitterProcessor.create(false);

    private String serverId;

    private Queue<Runnable> scheduleJobQueue = new ArrayDeque<>();

    private Map<String, LongAdder> transportCounter = new ConcurrentHashMap<>();

    @Getter
    @Setter
    private Map<String, Long> transportLimits = new ConcurrentHashMap<>();

    public void setTransportLimit(Transport transport, long limit) {
        transportLimits.put(transport.getId(), limit);
    }

    public void shutdown() {
        repository.values()
                .parallelStream()
                .map(DeviceSession::getId)
                .forEach(this::unregister);
    }

    @Override
    public boolean isOutOfMaximumSessionLimit(Transport transport) {
        long max = getMaximumSession(transport);
        return max > 0 && getCurrentSession(transport) >= max;
    }

    @Override
    public long getMaximumSession(Transport transport) {
        Long counter = transportLimits.get(transport.getId());
        return counter == null ? -1 : counter;
    }

    @Override
    public long getCurrentSession(Transport transport) {
        LongAdder counter = transportCounter.get(transport.getId());
        return counter == null ? 0 : counter.longValue();
    }

    public Mono<Long> checkSession() {
        AtomicLong startWith = new AtomicLong();
        return Flux.fromIterable(repository.values())
                .distinct()
                .publishOn(Schedulers.parallel())
                .filterWhen(session -> {
                    if (!session.isAlive()) {
                        return Mono.just(true);
                    }
                    return session
                            .getOperator()
                            .getConnectionServerId()
                            .switchIfEmpty(Mono.just(""))
                            .filter(s -> !serverId.equals(s))
                            .doOnNext((ignore) -> log.warn("device [{}] state error", session.getDeviceId()))
                            .flatMap(ignore -> session.getOperator().online(serverId, session.getId()))
                            .thenReturn(false);
                })
                .map(DeviceSession::getId)
                .doOnNext(this::unregister)
                .collect(Collectors.counting())
                .doOnNext((l) -> {
                    if (log.isInfoEnabled()) {
                        log.info("expired sessions:{}", l);
                    }
                })
                .name("session_checker:".concat(serverId))
                .metrics()
                .doOnError(err -> log.error(err.getMessage(), err))
                .doOnSubscribe(subscription -> {
                    log.info("start check session");
                    startWith.set(System.currentTimeMillis());
                })
                .doFinally(s -> {
                    //上报session数量
                    transportCounter.forEach((transport, number) -> gatewayServerMonitor.metrics().reportSession(transport, number.intValue()));
                    //执行任务
                    for (Runnable runnable = scheduleJobQueue.poll(); runnable != null; runnable = scheduleJobQueue.poll()) {
                        runnable.run();
                    }
//                    Map<String, Long> total = repository.values().stream()
//                            .collect(Collectors.groupingBy(session -> session.getTransport().getId(), Collectors.counting()));

//                    total.forEach((transport, number) -> gatewayServerMonitor.metrics().reportSession(transport, number.intValue()));

                    if (log.isInfoEnabled()) {
                        log.info("check session complete,current server sessions:{}.use time:{}ms.",
                                transportCounter,
                                System.currentTimeMillis() - startWith.get());

                    }
                });
    }

    public void init() {
        Objects.requireNonNull(gatewayServerMonitor, "gatewayServerMonitor");
        if (executorService == null) {
            executorService = Executors.newSingleThreadScheduledExecutor();
        }
        serverId = gatewayServerMonitor.getCurrentServerId();

        //每30秒检查一次设备连接情况
        executorService.scheduleAtFixedRate(() -> this.checkSession().subscribe(), 10, 30, TimeUnit.SECONDS);

    }

    @Override
    public DeviceSession getSession(String clientId) {
        DeviceSession session = repository.get(clientId);
        if (session == null || !session.isAlive()) {
            return null;
        }
        return session;
    }

    @Override
    public DeviceSession register(DeviceSession session) {
        DeviceSession old = repository.put(session.getDeviceId(), session);
        if (old != null) {
            //清空sessionId不同
            if (!old.getId().equals(old.getDeviceId())) {
                repository.remove(old.getId());
            }
        }
        if (!session.getId().equals(session.getDeviceId())) {
            repository.put(session.getId(), session);
        }
        if (null != old) {
            //1. 可能是多个设备使用了相同的id.
            //2. 可能是同一个设备,注销后立即上线,由于种种原因,先处理了上线后处理了注销逻辑.
            log.warn("device[{}] session exists,disconnect old session:{}", old.getDeviceId(), session);
            //加入关闭连接队列
            scheduleJobQueue.add(old::close);
        } else {
            //本地计数
            transportCounter
                    .computeIfAbsent(session.getTransport().getId(), transport -> new LongAdder())
                    .increment();
        }
        //注册中心上线
        session.getOperator()
                .online(serverId, session.getId())
                .subscribe();
        //通知
        if (onDeviceRegister.hasDownstreams()) {
            onDeviceRegister.onNext(session);
        }
        return old;
    }

    @Override
    public Flux<DeviceSession> onRegister() {
        return onDeviceRegister.map(Function.identity());
    }

    @Override
    public Flux<DeviceSession> onUnRegister() {
        return onDeviceUnRegister.map(Function.identity());
    }

    @Override
    public Flux<DeviceSession> getAllSession() {
        return Flux
                .fromIterable(repository.values())
                .distinct();
    }

    @Override
    public DeviceSession unregister(String idOrDeviceId) {
        DeviceSession client = repository.remove(idOrDeviceId);

        if (null != client) {
            if (!client.getId().equals(client.getDeviceId())) {
                repository.remove(client.getId().equals(idOrDeviceId) ? client.getDeviceId() : client.getId());
            }
            //本地计数
            transportCounter
                    .computeIfAbsent(client.getTransport().getId(), transport -> new LongAdder())
                    .decrement();
            //注册中心下线
            client.getOperator()
                    .offline()
                    .subscribe();
            //加入关闭连接队列
            scheduleJobQueue.add(client::close);
            //通知
            if (onDeviceRegister.hasDownstreams()) {
                onDeviceUnRegister.onNext(client);
            }
        }
        return client;
    }

}
