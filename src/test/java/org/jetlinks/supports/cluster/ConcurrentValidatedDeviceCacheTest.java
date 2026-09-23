package org.jetlinks.supports.cluster;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceProductOperator;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class ConcurrentValidatedDeviceCacheTest {

    @Test
    public void shouldExpireIdleEntriesAndExtendAccessedEntries() {
        AtomicLong clock = new AtomicLong(1_000);
        ConcurrentValidatedDeviceCache cache = new ConcurrentValidatedDeviceCache(
            Duration.ofNanos(1_000), clock::get
        );
        DeviceOperator device = mock(DeviceOperator.class);
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "device", device, Mono.just(mock(DeviceProductOperator.class)), cache
        );

        assertSame(device, source.block());
        clock.set(1_900);
        assertSame(source, cache.get("device"));
        clock.set(2_100);
        cache.cleanUp();
        assertEquals(1, cache.size());

        clock.set(2_900);
        cache.cleanUp();
        assertEquals(0, cache.size());
        assertSame(device, source.block());
        assertEquals(1, cache.size());
    }

    @Test
    public void shouldNotRemoveReplacementDuringScan() {
        AtomicLong clock = new AtomicLong(1_000);
        ConcurrentValidatedDeviceCache cache = new ConcurrentValidatedDeviceCache(
            Duration.ofNanos(1_000), clock::get
        );
        MonoValidatedDeviceOperator first = new MonoValidatedDeviceOperator(
            "device", mock(DeviceOperator.class), Mono.just(mock(DeviceProductOperator.class)), cache
        );
        MonoValidatedDeviceOperator replacement = new MonoValidatedDeviceOperator(
            "device", mock(DeviceOperator.class), Mono.just(mock(DeviceProductOperator.class)), cache
        );
        cache.put("device", first);
        clock.set(3_000);
        cache.put("device", replacement);
        cache.remove("device", first);
        cache.cleanUp();
        assertSame(replacement, cache.get("device"));
    }

    @Test
    public void shouldUseMaintenanceEpochForIdleExpiration() {
        AtomicLong clock = new AtomicLong(1_000);
        ConcurrentValidatedDeviceCache cache = new ConcurrentValidatedDeviceCache(
            Duration.ofSeconds(4), clock::get
        );
        MonoValidatedDeviceOperator source = new MonoValidatedDeviceOperator(
            "device", mock(DeviceOperator.class), Mono.just(mock(DeviceProductOperator.class)), cache
        );
        cache.put("device", source);

        clock.set(3_000_001_000L);
        cache.maintenance();
        assertSame(source, cache.get("device"));
        clock.set(4_000_001_001L);
        cache.maintenance();
        assertEquals(1, cache.size());

        clock.set(8_000_001_002L);
        cache.maintenance();
        assertEquals(0, cache.size());
    }
}
