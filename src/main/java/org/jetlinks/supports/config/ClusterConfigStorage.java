package org.jetlinks.supports.config;

import lombok.AllArgsConstructor;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.config.ConfigStorage;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ClusterConfigStorage implements ConfigStorage {

    ClusterCache<String, Object> cache;

    public ClusterCache<String, Object> getCache() {
        return cache;
    }

    @Override
    public Mono<Value> getConfig(String key) {
        if(StringUtils.isEmpty(key)){
            return Mono.empty();
        }
        return cache.get(key)
                .map(Value::simple);
    }

    @Override
    public Mono<Values> getConfigs(Collection<String> keys) {
        if(CollectionUtils.isEmpty(keys)){
            return Mono.empty();
        }
        return Flux.fromIterable(keys)
                .flatMap(key -> Mono.just(key).zipWith(cache.get(key)))
                .collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2))
                .map(Values::of);
    }

    @Override
    public Mono<Boolean> setConfigs(Map<String, Object> values) {
        if(CollectionUtils.isEmpty(values)){
            return Mono.just(true);
        }
        return cache.putAll(values);
    }

    @Override
    public Mono<Boolean> setConfig(String key, Object value) {
        if (value == null || key == null) {
            return Mono.just(true);
        }
        return cache.put(key, value);
    }

    @Override
    public Mono<Boolean> remove(String key) {
        return cache.remove(key);
    }

    @Override
    public Mono<Boolean> remove(Collection<String> key) {
        return cache.remove(key);
    }

    @Override
    public Mono<Boolean> clear() {
        return cache.clear()
                .thenReturn(true);
    }
}
