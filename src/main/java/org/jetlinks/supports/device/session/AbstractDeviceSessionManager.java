package org.jetlinks.supports.device.session;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.session.DeviceSessionEvent;
import org.jetlinks.core.device.session.DeviceSessionManager;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

@Slf4j
public abstract class AbstractDeviceSessionManager implements DeviceSessionManager {

    protected final Map<String, Mono<DeviceSession>> localSessions = new ConcurrentHashMap<>();

    private final List<Function<DeviceSessionEvent, Mono<Void>>> sessionEventHandlers = new CopyOnWriteArrayList<>();

    protected abstract Mono<DeviceSession> getRemoteSession(String deviceId);

    protected abstract Mono<Long> unregisterRemoteSession(String deviceId);

    protected abstract Mono<Long> getRemoteTotalSessions();

    public void init() {

    }

    @Override
    public Mono<DeviceSession> getSession(String deviceId, boolean onlyLocal) {
        Mono<DeviceSession> localSession = localSessions.get(deviceId);
        if (localSession == null) {
            return getRemoteSession(deviceId);
        }
        return localSession
                .filterWhen(session -> {
                    if (!session.isAlive()) {
                        return this
                                .doUnregister(session.getDeviceId(), true)
                                .thenReturn(false);
                    }
                    return Reactors.ALWAYS_TRUE;
                });
    }

    @Override
    public Mono<DeviceSession> register(DeviceSession session) {
        return this
                .doRegister(session)
                .thenReturn(session);
    }

    @Override
    public Mono<Void> unregister(String deviceId,
                                 boolean onlyLocal) {
        if (onlyLocal) {
            return doUnregister(deviceId, true);
        }
        return this
                .unregisterRemoteSession(deviceId)
                .then(doUnregister(deviceId, true));
    }

    @Override
    public Mono<Boolean> isAlive(String deviceId, boolean onlyLocal) {
        return this
                .getSession(deviceId, onlyLocal)
                .hasElement();
    }

    @Override
    public Mono<Long> totalSessions(boolean onlyLocal) {
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
    public Mono<Void> compute(String deviceId,
                              Function<Mono<DeviceSession>, Mono<DeviceSession>> computer) {

        return localSessions
                .compute(deviceId,
                         (_id, old) -> computer
                                 .apply(old)
                                 .flatMap(newSession -> {
                                     if (null == old) {
                                         return this
                                                 .doRegister(newSession)
                                                 .thenReturn(newSession);
                                     }
                                     return Mono.just(newSession);
                                 })
                                 .switchIfEmpty(Mono.fromRunnable(() -> localSessions.remove(deviceId)))
                                 .cache())
                .doOnError(err -> localSessions.remove(deviceId))
                .then();
    }

    private Mono<Void> doUnregister(String deviceId,
                                    boolean fireEvent) {
        Mono<DeviceSession> sessionMono = localSessions.remove(deviceId);
        if (null != sessionMono) {
            return sessionMono
                    .flatMap(session -> {
                        DeviceOperator device = session.getOperator();
                        Mono<Void> then = fireEvent ?
                                Mono.empty() :
                                fireEvent(
                                        DeviceSessionEvent.of(DeviceSessionEvent.Type.unregister, session)
                                );
                        if (device != null) {
                            return device
                                    .offline()
                                    .then(then);
                        }
                        return then;
                    });
        }
        return Mono.empty();
    }


    private Mono<Void> doRegister(DeviceSession session) {

        return Mono.empty();
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

    @Override
    public Disposable listenEvent(Function<DeviceSessionEvent, Mono<Void>> handler) {
        sessionEventHandlers.add(handler);
        return () -> sessionEventHandlers.remove(handler);
    }
}
