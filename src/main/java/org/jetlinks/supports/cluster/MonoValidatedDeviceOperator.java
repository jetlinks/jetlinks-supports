package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import org.jetlinks.core.device.DeviceOperator;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 首次订阅校验设备存在性，缓存有效期间直接返回设备操作对象。
 */
final class MonoValidatedDeviceOperator extends Mono<DeviceOperator> implements Scannable {

    private static final AtomicLongFieldUpdater<MonoValidatedDeviceOperator> INVALIDATION_VERSION =
        AtomicLongFieldUpdater.newUpdater(MonoValidatedDeviceOperator.class, "invalidationVersion");

    private static final AtomicLongFieldUpdater<MonoValidatedDeviceOperator> VALIDATED_VERSION =
        AtomicLongFieldUpdater.newUpdater(MonoValidatedDeviceOperator.class, "validatedVersion");

    private final String deviceId;
    private final DeviceOperator device;
    private final Mono<?> validator;
    private final Cache<String, Mono<DeviceOperator>> cache;

    private volatile long invalidationVersion;
    private volatile long validatedVersion = -1;

    MonoValidatedDeviceOperator(String deviceId,
                                DeviceOperator device,
                                Mono<?> validator,
                                Cache<String, Mono<DeviceOperator>> cache) {
        this.deviceId = Objects.requireNonNull(deviceId, "deviceId");
        this.device = Objects.requireNonNull(device, "device");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.cache = Objects.requireNonNull(cache, "cache");
    }

    @Override
    public void subscribe(@Nonnull CoreSubscriber<? super DeviceOperator> actual) {
        Mono<DeviceOperator> cached = cache.getIfPresent(deviceId);
        if (cached != this) {
            if (cached != null) {
                cached.subscribe(actual);
                return;
            }
            Mono<DeviceOperator> previous = cache.asMap().putIfAbsent(deviceId, this);
            if (previous != null && previous != this) {
                previous.subscribe(actual);
                return;
            }
        }

        long version = invalidationVersion;
        if (validatedVersion == version) {
            actual.onSubscribe(Operators.scalarSubscription(actual, device));
            return;
        }

        validator
            .map(ignore -> device)
            .doOnNext(ignore -> {
                if (invalidationVersion == version) {
                    VALIDATED_VERSION.set(this, version);
                }
            })
            .doOnSuccess(value -> {
                if (value == null) {
                    cache.asMap().remove(deviceId, this);
                }
            })
            .subscribe(actual);
    }

    void invalidate() {
        INVALIDATION_VERSION.incrementAndGet(this);
    }

    boolean isValidated() {
        return validatedVersion == invalidationVersion;
    }

    @Override
    public Object scanUnsafe(@Nonnull Attr attribute) {
        if (attribute == Attr.PARENT) {
            return validator;
        }
        return null;
    }
}
