package org.jetlinks.supports.protocol.management;

import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterProtocolSupportManager implements ProtocolSupportManager {

    private ClusterManager clusterManager;

    private ClusterCache<String, ProtocolSupportDefinition> cache;

    public ClusterProtocolSupportManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
        this.cache = clusterManager.getCache("__protocol_supports");
    }

    @Override
    public Mono<Boolean> store(Flux<ProtocolSupportDefinition> all) {
        return all.collect(Collectors.toMap(ProtocolSupportDefinition::getId, Function.identity()))
                .flatMap(cache::putAll);
    }

    @Override
    public Flux<ProtocolSupportDefinition> loadAll() {

        return cache.values();
    }

    @Override
    public Mono<Boolean> save(ProtocolSupportDefinition definition) {
        return clusterManager.getTopic("_protocol_changed")
                .publish(Mono.just(definition))
                .then(cache
                        .put(definition.getId(), definition));
    }

    @Override
    public Mono<Boolean> remove(String id) {
        return cache.remove(id);
    }
}
