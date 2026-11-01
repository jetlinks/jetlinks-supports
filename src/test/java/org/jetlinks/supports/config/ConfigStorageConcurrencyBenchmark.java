package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.event.InternalEventBus;
import org.openjdk.jmh.annotations.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Shared-key hits and per-thread cache reloads share one production manager. The latter
 * measures load processing independently of concurrent invalidation semantics.
 * The same compiled fixture runs against pre-observation and current production classes.
 * L2 is synchronous and in-process: measured latency is not Redis or device-message latency.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConfigStorageConcurrencyBenchmark {
    @State(Scope.Benchmark)
    public static class Shared {
        @Param({"-1"})
        public long ttlMillis;
        EventBusStorageManager manager;
        ClusterCache<String, Object> backend;
        LocalCacheClusterConfigStorage hot;
        List<String> keys = Arrays.asList("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7");

        @SuppressWarnings("unchecked")
        @Setup(Level.Trial)
        public void setup() throws Exception {
            manager = new EventBusStorageManager(mock(ClusterManager.class), new InternalEventBus());
            backend = (ClusterCache<String, Object>) Proxy.newProxyInstance(
                ClusterCache.class.getClassLoader(), new Class<?>[]{ClusterCache.class}, (proxy, method, arguments) -> {
                    if ("get".equals(method.getName())) {
                        if (arguments[0] instanceof String) return Mono.just("value");
                        Collection<String> requested = (Collection<String>) arguments[0];
                        return Flux.fromIterable(new ArrayList<>(requested))
                            .map(key -> new AbstractMap.SimpleImmutableEntry<>(key, (Object) "value"));
                    }
                    if ("toString".equals(method.getName())) return "LocalSynchronousL2";
                    throw new UnsupportedOperationException(method.getName());
                });
            hot = new LocalCacheClusterConfigStorage("shared-hot", manager, backend, ttlMillis, null, new ConcurrentHashMap<>());
            Values warmed = hot.getConfigs(keys).block();
            if (warmed == null || !"value".equals(warmed.getString("k0", ""))) throw new AssertionError("Warmup failed");
        }

        @TearDown(Level.Trial)
        public void dispose() {
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
            storage = new LocalCacheClusterConfigStorage("thread-load-" + Thread.currentThread().getName(),
                shared.manager, shared.backend, shared.ttlMillis, null, entries);
        }
    }

    @Benchmark
    public Value singleHit(Shared state) {
        return state.hot.getConfig("k0").block();
    }

    @Benchmark
    public Values batchHit8(Shared state) {
        return state.hot.getConfigs(state.keys).block();
    }

    @Benchmark
    public Value singleReload(Shared shared, Reload state) {
        // Both versions pay the same cache reset; this is not a notification benchmark.
        state.entries.clear();
        return state.storage.getConfig("k0").block();
    }

    @Benchmark
    public Values batchReload8(Shared shared, Reload state) {
        state.entries.clear();
        return state.storage.getConfigs(shared.keys).block();
    }
}
