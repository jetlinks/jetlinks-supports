package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Includes first load registration and removal, rather than reusing a warm storage registry. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConfigStorageChurnBenchmark {
    @State(Scope.Thread)
    public static class Churn {
        String id;
        CacheNotify clear;
        Consumer<LocalCacheClusterConfigStorage> remove;

        @Setup(Level.Trial)
        public void setup(ConfigStorageConcurrencyBenchmark.Shared shared) {
            id = "churn-" + Thread.currentThread().getName();
            clear = CacheNotify.clear(id);
            remove = storage -> shared.manager.cache.remove(id, storage);
        }

        LocalCacheClusterConfigStorage create(ConfigStorageConcurrencyBenchmark.Shared shared) {
            return new LocalCacheClusterConfigStorage(id, shared.manager, shared.backend, -1, remove);
        }
    }

    @Benchmark
    public Value singleNewStorage(ConfigStorageConcurrencyBenchmark.Shared shared, Churn state) {
        LocalCacheClusterConfigStorage storage = state.create(shared);
        Value value = storage.getConfig("k0").block();
        storage.clearLocalCache(state.clear);
        return value;
    }

    @Benchmark
    public Values batchNewStorage8(ConfigStorageConcurrencyBenchmark.Shared shared, Churn state) {
        LocalCacheClusterConfigStorage storage = state.create(shared);
        Values values = storage.getConfigs(shared.keys).block();
        storage.clearLocalCache(state.clear);
        return values;
    }
}
