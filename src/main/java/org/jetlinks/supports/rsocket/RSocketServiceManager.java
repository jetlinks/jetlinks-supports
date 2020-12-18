package org.jetlinks.supports.rsocket;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RSocketServiceManager {

    Mono<RSocketService> getService(String serviceName);

    Flux<RSocketService> getServices(String serviceName);

    Disposable register(RSocketService service);

}
