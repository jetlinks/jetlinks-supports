package org.jetlinks.supports.config;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Manager-scoped operational statistics; L1 hits have no observation work. Existing owners
 * sample loads at 1/1024 and record invalidation events. Snapshots expose the sample interval,
 * never full load counts. This class neither wraps publishers nor retains keys, values or errors.
 * A load attempt
 * includes the single-key fallback, while shared readers do not create another attempt.
 */
final class ConfigStorageMonitor {

    private static volatile CompositeType statisticsType;

    final LongAdder loadRequests = new LongAdder();
    final LongAdder loadKeys = new LongAdder();
    final LongAdder loadSuccesses = new LongAdder();
    final LongAdder loadFailures = new LongAdder();
    final LongAdder loadCancellations = new LongAdder();
    final AtomicLong loadsInFlight = new AtomicLong();
    final LongAdder loadNanos = new LongAdder();
    // Non-cumulative buckets: <=1ms, <=5ms, <=20ms, <=100ms, <=1s, >1s (all outcomes).
    final LongAdder[] loadBuckets = {new LongAdder(), new LongAdder(), new LongAdder(),
        new LongAdder(), new LongAdder(), new LongAdder()};
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
    // Fixed production ratio; tests use zero to deterministically exercise all terminal paths.
    int loadSampleMask = 1023;
    // Keep sampled writes separate from the read-mostly sampling policy.
    final AtomicLong lastLoadSuccessAt = new AtomicLong();
    volatile long lastNotificationAt;
    volatile long lastErrorAt;
    volatile String lastErrorType = "";

    void error(Throwable error) {
        lastErrorType = error.getClass().getName();
        lastErrorAt = System.currentTimeMillis();
    }

    long loadStarted(int keys) {
        if ((ThreadLocalRandom.current().nextInt() & loadSampleMask) != 0) {
            return 0;
        }
        loadRequests.increment();
        loadKeys.add(keys);
        loadsInFlight.incrementAndGet();
        return System.nanoTime();
    }

    /** The owning subscriber calls this once, before forwarding a terminal result. */
    void loadFinished(long started, Throwable error, boolean cancelled) {
        loadsInFlight.decrementAndGet();
        if (cancelled) {
            loadCancellations.increment();
        } else if (error != null) {
            loadFailures.increment();
            error(error);
        } else {
            loadSuccesses.increment();
            lastLoadSuccessAt.lazySet(System.currentTimeMillis());
        }
        long nanos = System.nanoTime() - started;
        loadNanos.add(nanos);
        int bucket = nanos <= 1_000_000 ? 0 : nanos <= 5_000_000 ? 1
            : nanos <= 20_000_000 ? 2 : nanos <= 100_000_000 ? 3 : nanos <= 1_000_000_000 ? 4 : 5;
        loadBuckets[bucket].increment();
    }

    CompositeData snapshot(Map<String, Object> values) {
        values.put("schemaVersion", 1L);
        values.put("instanceId", instanceId);
        values.put("capturedAt", System.currentTimeMillis());
        values.put("loadSampleInterval", (long) loadSampleMask + 1);
        values.put("sampledLoadRequests", loadRequests.sum());
        values.put("sampledLoadKeys", loadKeys.sum());
        values.put("sampledLoadSuccesses", loadSuccesses.sum());
        values.put("sampledLoadFailures", loadFailures.sum());
        values.put("sampledLoadCancellations", loadCancellations.sum());
        values.put("sampledLoadsInFlight", loadsInFlight.get());
        values.put("sampledLoadNanos", loadNanos.sum());
        for (int i = 0; i < loadBuckets.length; i++) {
            values.put("sampledLoadDurationBucket" + i, loadBuckets[i].sum());
        }
        values.put("notificationsReceived", notificationsReceived.sum());
        values.put("notificationProcessingFailures", notificationProcessingFailures.sum());
        values.put("notificationsWithoutStorage", notificationsWithoutStorage.sum());
        values.put("invalidatedEntries", invalidatedEntries.sum());
        values.put("invalidatedLoads", invalidatedLoads.sum());
        values.put("expiredEntries", expiredEntries.sum());
        values.put("refreshes", refreshes.sum());
        values.put("staleFallbacks", staleFallbacks.sum());
        values.put("rejectedLoadResults", rejectedLoadResults.sum());
        values.put("lastSampledLoadSuccessAt", lastLoadSuccessAt.get());
        values.put("lastNotificationAt", lastNotificationAt);
        values.put("lastErrorAt", lastErrorAt);
        values.put("lastErrorType", lastErrorType);
        CompositeType type = statisticsType;
        if (type == null) {
            statisticsType = type = createType("ConfigStorageStatistics", values);
        }
        return openData(type, values);
    }

    // Fixed schemas contain only field names/types, never runtime values or business keys.
    static CompositeType createType(String name, Map<String, Object> values) {
        String[] names = values.keySet().toArray(new String[0]);
        OpenType<?>[] types = new OpenType<?>[names.length];
        for (int i = 0; i < names.length; i++) {
            Object value = values.get(names[i]);
            types[i] = value instanceof String ? SimpleType.STRING
                : value instanceof Boolean ? SimpleType.BOOLEAN : SimpleType.LONG;
        }
        try {
            return new CompositeType(name, name, names, names, types);
        } catch (OpenDataException error) {
            throw new IllegalStateException("Invalid config storage statistics schema", error);
        }
    }

    static CompositeData openData(CompositeType type, Map<String, Object> values) {
        try {
            return new CompositeDataSupport(type, values);
        } catch (OpenDataException error) {
            throw new IllegalStateException("Invalid config storage statistics values", error);
        }
    }

}
