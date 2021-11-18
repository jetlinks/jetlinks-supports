package org.jetlinks.supports.rpc;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;


@Service
public interface TestApi {
    @ServiceMethod
    Mono<String> getData();
}