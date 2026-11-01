package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.event.InternalEventBus;
import org.openjdk.jmh.annotations.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/** Eight readers join one held L2 request; another thread releases it after all subscribe. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConfigStorageAsyncSharedBenchmark {
    @State(Scope.Benchmark)
    public static class Shared {
        EventBusStorageManager manager;
        Scheduler scheduler;
        final List<String> keys = Arrays.asList("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7");
        @Setup(Level.Trial) public void setup() {
            manager = new EventBusStorageManager(mock(ClusterManager.class), new InternalEventBus());
            scheduler = Schedulers.newParallel("config-shared-bench", 4);
        }
        @TearDown(Level.Trial) public void close() { manager.dispose(); scheduler.dispose(); }
    }

    @State(Scope.Thread)
    public static class Round {
        LocalCacheClusterConfigStorage storage;
        Map<String, LocalCacheClusterConfigStorage.Cache> entries;
        Sinks.One<Integer> gate;
        int subscriptions;
        int requests;
        @SuppressWarnings("unchecked")
        @Setup(Level.Trial) public void setup(Shared shared) {
            ClusterCache<String, Object> backend = (ClusterCache<String, Object>) Proxy.newProxyInstance(
                ClusterCache.class.getClassLoader(), new Class<?>[]{ClusterCache.class}, (proxy, method, args) -> {
                    if ("get".equals(method.getName())) {
                        requests++;
                        if (args[0] instanceof String) return gate.asMono().map(ignored -> "value");
                        List<String> keys = new ArrayList<>((Collection<String>) args[0]);
                        return gate.asMono().flatMapMany(ignored -> Flux.fromIterable(keys)
                            .map(key -> new AbstractMap.SimpleImmutableEntry<>(key, (Object) "value")));
                    }
                    if ("toString".equals(method.getName())) return "HeldAsyncL2";
                    throw new UnsupportedOperationException(method.getName());
                });
            entries = new ConcurrentHashMap<>();
            storage = new LocalCacheClusterConfigStorage("async-" + Thread.currentThread().getName(),
                shared.manager, backend, -1, null, entries);
        }
        void reset() { entries.clear(); gate = Sinks.one(); subscriptions = 0; requests = 0; }
        void subscribed(Shared shared) {
            if (++subscriptions == 8) {
                Sinks.One<Integer> signal = gate;
                shared.scheduler.schedule(() -> signal.tryEmitValue(1));
            }
        }
    }

    @Benchmark public Object[] singleShared8(Shared shared, Round round) {
        round.reset();
        List<Mono<Value>> reads = new ArrayList<>(8);
        for (int i = 0; i < 8; i++) reads.add(round.storage.getConfig("k0")
            .doOnSubscribe(ignored -> round.subscribed(shared)));
        Object[] values = Mono.zip(reads, result -> result).block();
        if (round.requests != 1 || values.length != 8) throw new AssertionError("Single sharing failed");
        for (Object value : values) if (!"value".equals(((Value) value).asString()))
            throw new AssertionError("Wrong single result");
        return values;
    }

    @Benchmark public Object[] batchShared8(Shared shared, Round round) {
        round.reset();
        List<Mono<Values>> reads = new ArrayList<>(8);
        for (int i = 0; i < 8; i++) reads.add(round.storage.getConfigs(shared.keys)
            .doOnSubscribe(ignored -> round.subscribed(shared)));
        Object[] values = Mono.zip(reads, result -> result).block();
        if (round.requests != 1 || values.length != 8) throw new AssertionError("Batch sharing failed");
        for (Object value : values) if (!"value".equals(((Values) value).getString("k7", "")))
            throw new AssertionError("Wrong batch result");
        return values;
    }
}
