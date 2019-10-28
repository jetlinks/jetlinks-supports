package org.jetlinks.supports.protocol.management;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ProtocolSupportManager {

    Mono<Boolean> store(Flux<ProtocolSupportDefinition> all);

    Flux<ProtocolSupportDefinition> loadAll();

    Mono<Boolean> save(ProtocolSupportDefinition definition);

    Mono<Boolean> remove(String id);
}
