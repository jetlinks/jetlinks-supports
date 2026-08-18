package org.jetlinks.supports.config;

import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.openjdk.jmh.annotations.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link LocalCacheClusterConfigStorage#getConfigs(Collection)} micro benchmark.
 *
 * The hit scenarios measure the production L1 path without a mock invocation in the measured
 * operation. Miss scenarios retain the mocked L2 boundary so that map assembly and subscriber
 * behavior remain covered without introducing an external Redis dependency.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class LocalCacheClusterConfigStorageBenchmark {

    @Benchmark
    public Values positiveHit8(PositiveHitState state) {
        return state.storage.getConfigs(state.keys).block();
    }

    @Benchmark
    public Values negativeHit1(NegativeHitState state) {
        return state.storage.getConfigs(state.keys).block();
    }

    @Benchmark
    public Values mixedHit8(MixedHitState state) {
        return state.storage.getConfigs(state.keys).block();
    }

    @Benchmark
    public Values partialMiss8(PartialMissState state) {
        return state.storage.getConfigs(state.keys).block();
    }

    @Benchmark
    public Values allMiss8(AllMissState state) {
        return state.storage.getConfigs(state.keys).block();
    }

    abstract static class BaseState {
        LocalCacheClusterConfigStorage storage;
        List<String> keys;
        Map<String, Object> backend;

        void initialize(int presentKeys) {
            keys = Arrays.asList("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7");
            backend = new HashMap<>();
            for (int i = 0; i < presentKeys; i++) {
                backend.put(keys.get(i), "value-" + i);
            }

            @SuppressWarnings("unchecked")
            ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
            when(clusterCache.get(any(Collection.class)))
                .thenAnswer(invocation -> load(invocation.getArgument(0)));

            EventBusStorageManager manager = mock(EventBusStorageManager.class);
            when(manager.doNotify(any())).thenReturn(Mono.empty());

            storage = new LocalCacheClusterConfigStorage(
                "benchmark",
                manager,
                clusterCache,
                -1,
                null,
                new ConcurrentHashMap<>()
            );
        }

        private Flux<Map.Entry<String, Object>> load(Collection<String> requested) {
            return Flux
                // ClusterCache owns the request collection after get(Collection) returns. Snapshot
                // it here just as a remote implementation does before emitting entries.
                .fromIterable(new ArrayList<>(requested))
                .filter(backend::containsKey)
                .map(key -> new AbstractMap.SimpleImmutableEntry<>(key, backend.get(key)));
        }

        void warmCache() {
            storage.getConfigs(keys).block();
        }
    }

    @State(Scope.Thread)
    public static class PositiveHitState extends BaseState {
        @Setup(Level.Trial)
        public void setup() {
            initialize(8);
            warmCache();
        }
    }

    @State(Scope.Thread)
    public static class NegativeHitState extends BaseState {
        @Setup(Level.Trial)
        public void setup() {
            initialize(0);
            keys = Collections.singletonList("missing");
            warmCache();
        }
    }

    @State(Scope.Thread)
    public static class MixedHitState extends BaseState {
        @Setup(Level.Trial)
        public void setup() {
            initialize(6);
            warmCache();
        }
    }

    @State(Scope.Thread)
    public static class PartialMissState extends BaseState {
        @Setup(Level.Trial)
        public void setup() {
            initialize(8);
            warmCache();
        }

        @Setup(Level.Invocation)
        public void expireOneKey() {
            storage.clearLocalCache(CacheNotify.expires("benchmark", Collections.singleton("k7")));
        }
    }

    @State(Scope.Thread)
    public static class AllMissState extends BaseState {
        @Setup(Level.Trial)
        public void setup() {
            initialize(8);
            warmCache();
        }

        @Setup(Level.Invocation)
        public void expireAllKeys() {
            storage.clearLocalCache(CacheNotify.expires("benchmark", keys));
        }
    }
}
