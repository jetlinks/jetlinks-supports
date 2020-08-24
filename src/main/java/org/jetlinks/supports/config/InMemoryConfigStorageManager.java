package org.jetlinks.supports.config;

import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryConfigStorageManager implements ConfigStorageManager {

    private final Map<String, ConfigStorage> storageMap = new ConcurrentHashMap<>();

    @Override
    public Mono<ConfigStorage> getStorage(String id) {
        return Mono.just(storageMap.computeIfAbsent(id, __ -> new InMemoryConfigStorage()));
    }
}
