package org.jetlinks.supports.config;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Manager-scoped operational statistics. L1 decisions are sampled per API call; actual
 * asynchronous loads and invalidations are counted independently. No key, value, Throwable
 * or per-entry monitoring state is retained. Snapshots use standard JMX OpenData.
 */
final class ConfigStorageMonitor {

    final LongAdder loadRequests = new LongAdder();
    final LongAdder loadKeys = new LongAdder();
    final LongAdder loadSuccesses = new LongAdder();
    final LongAdder loadFailures = new LongAdder();
    final LongAdder loadCancellations = new LongAdder();
    final LongAdder writeSuccesses = new LongAdder();
    final LongAdder writeFailures = new LongAdder();
    final AtomicLong loadsInFlight = new AtomicLong();
    final LongAdder loadNanos = new LongAdder();
    // Non-cumulative buckets: <=1ms, <=5ms, <=20ms, <=100ms, <=1s, >1s (all outcomes).
    final LongAdder[] loadBuckets = {new LongAdder(), new LongAdder(), new LongAdder(),
        new LongAdder(), new LongAdder(), new LongAdder()};
    final LongAdder notificationsSent = new LongAdder();
    final LongAdder notificationFailures = new LongAdder();
    final LongAdder notificationsReceived = new LongAdder();
    final LongAdder notificationProcessingFailures = new LongAdder();
    final LongAdder notificationsWithoutStorage = new LongAdder();
    final LongAdder invalidatedEntries = new LongAdder();
    final LongAdder invalidatedLoads = new LongAdder();
    final LongAdder expiredEntries = new LongAdder();
    final LongAdder refreshes = new LongAdder();
    final LongAdder staleFallbacks = new LongAdder();
    final LongAdder rejectedLoadResults = new LongAdder();
    private final String instanceId = UUID.randomUUID().toString();
    // Disabled by default: the fastest L1 hits must not pay for random sampling.
    volatile ReadCounters reads;
    private volatile ReadCounters disabledReads = new ReadCounters(0);
    volatile long lastLoadSuccessAt;
    volatile long lastNotificationAt;
    volatile long lastErrorAt;
    volatile String lastErrorType = "";

    ReadCounters sampleRead() {
        ReadCounters current = reads;
        return current != null && ThreadLocalRandom.current().nextInt(current.interval) == 0 ? current : null;
    }

    synchronized void setReadSamplingEnabled(boolean enabled) {
        if ((reads != null) != enabled) {
            if (enabled) {
                reads = new ReadCounters(1024);
            } else {
                disabledReads = new ReadCounters(0);
                reads = null;
            }
        }
    }

    void error(Throwable error) {
        lastErrorType = error.getClass().getName();
        lastErrorAt = System.currentTimeMillis();
    }

    <T> Mono<T> observeLoad(Mono<T> source) {
        return Mono.defer(() -> {
            Load load = new Load(1);
            return source
                .doOnSuccess(ignored -> load.finish(0, null))
                .doOnError(error -> load.finish(1, error))
                .doFinally(ignored -> load.finish(2, null));
        });
    }

    <T> Mono<T> observeWrite(Mono<T> source) {
        return source
            .doOnSuccess(ignored -> writeSuccesses.increment())
            .doOnError(error -> {
                writeFailures.increment();
                error(error);
            });
    }

    <T> Flux<T> observeLoad(Flux<T> source, int keys) {
        return Flux.defer(() -> {
            Load load = new Load(keys);
            return source
                .doOnComplete(() -> load.finish(0, null))
                .doOnError(error -> load.finish(1, error))
                .doFinally(ignored -> load.finish(2, null));
        });
    }

    CompositeData snapshot(Map<String, Object> values) {
        ReadCounters sample = reads;
        if (sample == null) {
            sample = disabledReads;
        }
        values.put("schemaVersion", 1L);
        values.put("instanceId", instanceId);
        values.put("statisticsEpoch", sample.epoch);
        values.put("capturedAt", System.currentTimeMillis());
        values.put("readSampleInterval", (long) sample.interval);
        values.put("sampledReadCalls", sample.calls.sum());
        values.put("valueHitKeys", sample.valueHits.sum());
        values.put("negativeHitKeys", sample.negativeHits.sum());
        values.put("missKeys", sample.misses.sum());
        values.put("loadRequests", loadRequests.sum());
        values.put("loadKeys", loadKeys.sum());
        values.put("loadSuccesses", loadSuccesses.sum());
        values.put("loadFailures", loadFailures.sum());
        values.put("loadCancellations", loadCancellations.sum());
        values.put("writeSuccesses", writeSuccesses.sum());
        values.put("writeFailures", writeFailures.sum());
        values.put("loadsInFlight", loadsInFlight.get());
        values.put("loadNanos", loadNanos.sum());
        for (int i = 0; i < loadBuckets.length; i++) {
            values.put("loadDurationBucket" + i, loadBuckets[i].sum());
        }
        values.put("notificationsSent", notificationsSent.sum());
        values.put("notificationFailures", notificationFailures.sum());
        values.put("notificationsReceived", notificationsReceived.sum());
        values.put("notificationProcessingFailures", notificationProcessingFailures.sum());
        values.put("notificationsWithoutStorage", notificationsWithoutStorage.sum());
        values.put("invalidatedEntries", invalidatedEntries.sum());
        values.put("invalidatedLoads", invalidatedLoads.sum());
        values.put("expiredEntries", expiredEntries.sum());
        values.put("refreshes", refreshes.sum());
        values.put("staleFallbacks", staleFallbacks.sum());
        values.put("rejectedLoadResults", rejectedLoadResults.sum());
        values.put("lastLoadSuccessAt", lastLoadSuccessAt);
        values.put("lastNotificationAt", lastNotificationAt);
        values.put("lastErrorAt", lastErrorAt);
        values.put("lastErrorType", lastErrorType);
        return openData("ConfigStorageStatistics", values);
    }

    static CompositeData openData(String name, Map<String, Object> values) {
        String[] names = values.keySet().toArray(new String[0]);
        OpenType<?>[] types = new OpenType<?>[names.length];
        for (int i = 0; i < names.length; i++) {
            Object value = values.get(names[i]);
            types[i] = value instanceof String ? SimpleType.STRING
                : value instanceof Boolean ? SimpleType.BOOLEAN : SimpleType.LONG;
        }
        try {
            CompositeType type = new CompositeType(name, name, names, names, types);
            return new CompositeDataSupport(type, values);
        } catch (OpenDataException error) {
            throw new IllegalStateException("Invalid config storage statistics schema", error);
        }
    }

    /** One shared counter set per sampling epoch; a sampled read allocates no context object. */
    static final class ReadCounters {
        final String epoch = UUID.randomUUID().toString();
        final int interval;
        final LongAdder calls = new LongAdder();
        final LongAdder valueHits = new LongAdder();
        final LongAdder negativeHits = new LongAdder();
        final LongAdder misses = new LongAdder();

        ReadCounters(int interval) {
            this.interval = interval;
        }

        void record(int hits, int negatives, int missing) {
            calls.increment();
            valueHits.add(hits);
            negativeHits.add(negatives);
            misses.add(missing);
        }

    }

    /** Records one physical attempt, including cancellation before a result arrives. */
    private final class Load {
        private final long started = System.nanoTime();
        volatile int ended;

        Load(int keys) {
            loadRequests.increment();
            loadKeys.add(keys);
            loadsInFlight.incrementAndGet();
        }

        void finish(int outcome, Throwable error) {
            if (!ENDED.compareAndSet(this, 0, 1)) {
                return;
            }
            loadsInFlight.decrementAndGet();
            if (outcome == 0) {
                loadSuccesses.increment();
                lastLoadSuccessAt = System.currentTimeMillis();
            } else if (outcome == 1) {
                loadFailures.increment();
                error(error);
            } else {
                loadCancellations.increment();
            }
            long nanos = System.nanoTime() - started;
            loadNanos.add(nanos);
            int bucket = nanos <= 1_000_000 ? 0 : nanos <= 5_000_000 ? 1
                : nanos <= 20_000_000 ? 2 : nanos <= 100_000_000 ? 3 : nanos <= 1_000_000_000 ? 4 : 5;
            loadBuckets[bucket].increment();
        }
    }

    private static final AtomicIntegerFieldUpdater<Load> ENDED =
        AtomicIntegerFieldUpdater.newUpdater(Load.class, "ended");
}
