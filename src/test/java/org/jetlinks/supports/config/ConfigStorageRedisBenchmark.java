package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.redis.RedisClusterCache;
import org.jetlinks.supports.event.InternalEventBus;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/** Same compiled fixture for both runtimes; real Redis 5, independent per-thread L1. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ConfigStorageRedisBenchmark {
    @State(Scope.Benchmark)
    public static class Shared {
        EventBusStorageManager manager;
        RedisClusterCache<String, Object> backend;
        List<String> keys = Arrays.asList("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7");

        @Setup(Level.Trial)
        public void setup() {
            manager = new EventBusStorageManager(mock(ClusterManager.class), new InternalEventBus());
            backend = new RedisClusterCache<>("lifecycle-perf-" + UUID.randomUUID(), RedisHelper.getRedisTemplate());
            Map<String, Object> values = new LinkedHashMap<>();
            for (String key : keys) values.put(key, "value");
            if (!Boolean.TRUE.equals(backend.putAll(values).block())) throw new AssertionError("Redis setup failed");
        }

        @TearDown(Level.Trial)
        public void dispose() {
            backend.clear().block();
            manager.dispose();
        }
    }

    @State(Scope.Thread)
    public static class Reload {
        LocalCacheClusterConfigStorage storage;
        Map<String, LocalCacheClusterConfigStorage.Cache> entries;

        @Setup(Level.Trial)
        public void setup(Shared shared) {
            entries = new ConcurrentHashMap<>();
            storage = new LocalCacheClusterConfigStorage("redis-load-" + Thread.currentThread().getName(),
                shared.manager, shared.backend, -1, null, entries);
        }
    }

    @Benchmark
    public Object directSingle(Shared shared) {
        return shared.backend.get("k0").block();
    }

    @Benchmark
    public Value singleReload(Shared shared, Reload state) {
        state.entries.clear();
        return state.storage.getConfig("k0").block();
    }

    @Benchmark
    public Values batchReload8(Shared shared, Reload state) {
        state.entries.clear();
        return state.storage.getConfigs(shared.keys).block();
    }
}
