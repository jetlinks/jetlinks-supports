package org.jetlinks.supports.scalecube;

import reactor.core.publisher.Mono;

public class TestApiImpl implements TestApi{
    @Override
    public Mono<String> lowercase(Long data) {
        return Mono.just(String.valueOf(data));
    }

    @Override
    public Mono<String> add(Long[] data) {
        return Mono.just(String.valueOf(Math.addExact(data[0],data[1])));
    }
}
