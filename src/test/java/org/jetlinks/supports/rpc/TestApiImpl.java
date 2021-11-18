package org.jetlinks.supports.rpc;

import lombok.AllArgsConstructor;
import reactor.core.publisher.Mono;

@AllArgsConstructor
public class TestApiImpl implements TestApi {
    private String data;

    public Mono<String> getData() {
        return Mono.just(data);
    }
}