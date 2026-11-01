package org.jetlinks.supports.config;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/** Alternating direct/storage timings share the same client, fork and worker population. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConfigStorageRedisPairBenchmark {
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Timings {
        public long directNanos;
        public long storageNanos;
        private boolean storageFirst;
        @Setup(Level.Iteration) public void reset() { directNanos = 0; storageNanos = 0; storageFirst = false; }
    }

    @Benchmark public void pairedSingle(ConfigStorageRedisBenchmark.Shared shared,
                                       ConfigStorageRedisBenchmark.Reload reload,
                                       Timings timings, Blackhole blackhole) {
        if (timings.storageFirst) {
            storage(reload, timings, blackhole);
            direct(shared, timings, blackhole);
        } else {
            direct(shared, timings, blackhole);
            storage(reload, timings, blackhole);
        }
        timings.storageFirst = !timings.storageFirst;
    }

    private void direct(ConfigStorageRedisBenchmark.Shared shared, Timings timings, Blackhole blackhole) {
        long start = System.nanoTime();
        Object value = shared.backend.get("k0").block();
        timings.directNanos += System.nanoTime() - start;
        blackhole.consume(value);
    }

    private void storage(ConfigStorageRedisBenchmark.Reload reload, Timings timings, Blackhole blackhole) {
        long start = System.nanoTime();
        reload.entries.clear();
        Object value = reload.storage.getConfig("k0").block();
        timings.storageNanos += System.nanoTime() - start;
        blackhole.consume(value);
    }

    @Benchmark public void pairedBatch8(ConfigStorageRedisBenchmark.Shared shared,
                                       ConfigStorageRedisBenchmark.Reload reload,
                                       Timings timings, Blackhole blackhole) {
        if (timings.storageFirst) {
            storageBatch(shared, reload, timings, blackhole);
            directBatch(shared, timings, blackhole);
        } else {
            directBatch(shared, timings, blackhole);
            storageBatch(shared, reload, timings, blackhole);
        }
        timings.storageFirst = !timings.storageFirst;
    }

    private void directBatch(ConfigStorageRedisBenchmark.Shared shared, Timings timings, Blackhole blackhole) {
        long start = System.nanoTime();
        Object value = shared.backend.get(shared.keys).collectList().block();
        timings.directNanos += System.nanoTime() - start;
        blackhole.consume(value);
    }

    private void storageBatch(ConfigStorageRedisBenchmark.Shared shared, ConfigStorageRedisBenchmark.Reload reload,
                              Timings timings, Blackhole blackhole) {
        long start = System.nanoTime();
        reload.entries.clear();
        Object value = reload.storage.getConfigs(shared.keys).block();
        timings.storageNanos += System.nanoTime() - start;
        blackhole.consume(value);
    }
}
