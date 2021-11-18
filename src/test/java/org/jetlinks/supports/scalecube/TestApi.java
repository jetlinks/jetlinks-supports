package org.jetlinks.supports.scalecube;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service("org.jetlinks.supports.scalecube.TestApi")
public interface TestApi {

     @ServiceMethod("lowercase")
     Mono<String> lowercase(Long data);

     @ServiceMethod
     Mono<String> add(Long[] data);

}
