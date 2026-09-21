package org.jetlinks.supports.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class ConcurrentValidatedDeviceCache {

    private final ConcurrentHashMap<String, MonoValidatedDeviceOperator> entries = new ConcurrentHashMap<>();
    private final long expireAfterAccessNanos;
    private final long accessSampleNanos;
    private final long scanIntervalNanos;
    private final LongSupplier clock;
    private volatile long accessEpochNanos;
    private long lastScanNanos;

    ConcurrentValidatedDeviceCache(Duration expireAfterAccess) {
        this(expireAfterAccess, System::nanoTime);
    }

    ConcurrentValidatedDeviceCache(Duration expireAfterAccess, LongSupplier clock) {
        if (expireAfterAccess.isNegative() || expireAfterAccess.isZero()) {
            throw new IllegalArgumentException("expireAfterAccess must be positive");
        }
        this.expireAfterAccessNanos = expireAfterAccess.toNanos();
        this.accessSampleNanos = Math.max(1, Math.min(expireAfterAccessNanos / 4, TimeUnit.SECONDS.toNanos(1)));
        this.scanIntervalNanos = Math.max(TimeUnit.SECONDS.toNanos(1),
                                          Math.min(expireAfterAccessNanos, TimeUnit.MINUTES.toNanos(1)));
        this.clock = clock;
        this.accessEpochNanos = clock.getAsLong();
        this.lastScanNanos = accessEpochNanos;
    }

    MonoValidatedDeviceOperator get(String deviceId) {
        MonoValidatedDeviceOperator source = entries.get(deviceId);
        if (source != null) {
            long now = accessSampleNanos == TimeUnit.SECONDS.toNanos(1)
                ? accessEpochNanos
                : clock.getAsLong();
            source.touch(now, accessSampleNanos);
        }
        return source;
    }

    MonoValidatedDeviceOperator putIfAbsent(String deviceId, MonoValidatedDeviceOperator source) {
        source.touch(clock.getAsLong(), 0);
        return entries.putIfAbsent(deviceId, source);
    }

    void put(String deviceId, MonoValidatedDeviceOperator source) {
        source.touch(clock.getAsLong(), 0);
        entries.put(deviceId, source);
    }

    boolean remove(String deviceId, MonoValidatedDeviceOperator source) {
        return entries.remove(deviceId, source);
    }

    void invalidateAll() {
        for (MonoValidatedDeviceOperator source : entries.values()) {
            source.invalidate();
        }
        entries.clear();
    }

    void cleanUp() {
        long now = clock.getAsLong();
        accessEpochNanos = now;
        cleanUp(now);
    }

    void maintenance() {
        long now = clock.getAsLong();
        accessEpochNanos = now;
        if (now - lastScanNanos >= scanIntervalNanos) {
            lastScanNanos = now;
            cleanUp(now);
        }
    }

    private void cleanUp(long now) {
        for (Map.Entry<String, MonoValidatedDeviceOperator> entry : entries.entrySet()) {
            MonoValidatedDeviceOperator source = entry.getValue();
            if (source.isIdle(now, expireAfterAccessNanos)) {
                entries.computeIfPresent(entry.getKey(), (ignore, current) ->
                    current == source && current.isIdle(clock.getAsLong(), expireAfterAccessNanos)
                        ? null : current);
            }
        }
    }

    int size() {
        return entries.size();
    }
}
