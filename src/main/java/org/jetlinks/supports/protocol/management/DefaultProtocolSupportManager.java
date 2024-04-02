package org.jetlinks.supports.protocol.management;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.event.Subscription;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.supports.protocol.StaticProtocolSupports;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class DefaultProtocolSupportManager extends StaticProtocolSupports implements ProtocolSupportManager {
    public static final String topic = "/_sys/protocol/changed";

    private final EventBus eventBus;
    private final ClusterCache<String, ProtocolSupportDefinition> cache;
    private final ProtocolSupportLoader loader;
    private final Map<String, String> configProtocolIdMapping = new ConcurrentHashMap<>();

    private final Disposable.Composite disposable = Disposables.composite();

    @Setter
    private Duration loadTimeout = Duration.ofSeconds(30);

    public void init() {

        //通过事件总线来传递协议变更事件
        disposable.add(
            eventBus.subscribe(
                Subscription
                    .builder()
                    .topics(topic)
                    .subscriberId("protocol-manager")
                    .local()
                    .broker()
                    .build(),
                payload -> init(payload.decode(ProtocolSupportDefinition.class)))
        );

        try {
            loadAll()
                .filter(de -> de.getState() == 1)
                .flatMap(def -> this
                    .init(def)
                    .onErrorResume(err -> Mono.empty()))
                .blockLast(loadTimeout);
        } catch (Throwable error) {
            log.warn("load protocol error", error);
        }
    }


    public void shutdown() {
        disposable.dispose();
    }

    @Override
    public Mono<Boolean> store(Flux<ProtocolSupportDefinition> all) {
        return all
            .collect(Collectors.toMap(ProtocolSupportDefinition::getId, Function.identity()))
            .flatMap(cache::putAll);
    }

    @Override
    public Flux<ProtocolSupportDefinition> loadAll() {
        return cache.values();
    }

    @Override
    public Mono<Boolean> save(ProtocolSupportDefinition definition) {
        return cache
            .put(definition.getId(), definition)
            .flatMap(su -> eventBus
                .publish(topic, definition)
                .thenReturn(su));
    }

    @Override
    public Mono<Boolean> remove(String id) {
        return cache
            .get(id)
            .doOnNext(def -> def.setState((byte) -1))
            .flatMap(e -> eventBus.publish(topic, e))
            .then(cache.remove(id));
    }

    /**
     * 初始化协议
     * <p>
     * 如果{@link ProtocolSupportDefinition#getState()}不为1，则表示卸载协议。为0则表示加载协议.
     *
     * @param definition 协议定义
     */
    public Mono<Void> init(ProtocolSupportDefinition definition) {
        return Mono
            .defer(() -> {
                String operation = definition.getState() != 1 ? "uninstall" : "install";
                try {
                    if (definition.getState() != 1) {
                        String protocol = configProtocolIdMapping.get(definition.getId());
                        if (protocol != null) {
                            log.debug("uninstall protocol:{}", definition);
                            unRegister(protocol);
                            return Mono.empty();
                        }
                    }
                    Consumer<ProtocolSupport> consumer = definition.getState() != 1 ? this::unRegister : this::register;

                    log.debug("{} protocol:{}", operation, definition);

                    return loader
                        .load(definition)
                        .doOnNext(e -> {
                            e.init(definition.getConfiguration());
                            log.debug("{} protocol[{}] success: {}", operation, definition.getId(), e);
                            configProtocolIdMapping.put(definition.getId(), e.getId());
                            consumer.accept(afterLoaded(definition, e));
                        })
                        .onErrorResume((e) -> {
                            log.error("{} protocol[{}] error", operation, definition.getId(), e);
                            loadError(definition, e);
                            return Mono.empty();
                        })
                        .then();
                } catch (Throwable err) {
                    log.error("{} protocol error", operation, err);
                    loadError(definition, err);
                }
                return Mono.empty();
            })
            .as(MonoTracer.create("/protocol/" + definition.getId() + "/init"));
    }

    protected void loadError(ProtocolSupportDefinition def, Throwable err) {

    }

    protected ProtocolSupport afterLoaded(ProtocolSupportDefinition def,
                                          ProtocolSupport support) {
        return support;
    }
}
