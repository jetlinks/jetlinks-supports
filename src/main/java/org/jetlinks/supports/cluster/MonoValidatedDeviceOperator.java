package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import org.jetlinks.core.device.DeviceOperator;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

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

        if (validatedVersion == invalidationVersion) {
            // 标量结果须在 request 时再次核对失效状态，不能在零 demand 订阅时提前固定旧设备。
            actual.onSubscribe(new ValidatedSubscription(actual, this));
            return;
        }

        validation(invalidationVersion).subscribe(actual);
    }

    private Mono<DeviceOperator> validation(long version) {
        return validator
            .map(ignore -> device)
            .doOnNext(ignore -> {
                if (invalidationVersion == version && cache.getIfPresent(deviceId) == this) {
                    VALIDATED_VERSION.set(this, version);
                }
            })
            .doOnSuccess(value -> {
                if (value == null) {
                    cache.asMap().remove(deviceId, this);
                }
            });
    }

    private static final class ValidatedSubscription implements Subscription, CoreSubscriber<DeviceOperator> {

        private static final AtomicIntegerFieldUpdater<ValidatedSubscription> STATE =
            AtomicIntegerFieldUpdater.newUpdater(ValidatedSubscription.class, "state");

        private static final int FRESH = 0;
        private static final int REQUESTED = 1;
        private static final int CANCELLED = 2;
        private static final int TERMINATED = 3;

        private final CoreSubscriber<? super DeviceOperator> actual;
        private final MonoValidatedDeviceOperator owner;
        private volatile Subscription fallback;
        private volatile int state;

        private ValidatedSubscription(CoreSubscriber<? super DeviceOperator> actual,
                                      MonoValidatedDeviceOperator owner) {
            this.actual = actual;
            this.owner = owner;
        }

        @Override
        public void request(long count) {
            if (!Operators.validate(count) || !STATE.compareAndSet(this, FRESH, REQUESTED)) {
                return;
            }
            long version = owner.invalidationVersion;
            if (owner.validatedVersion == version && owner.invalidationVersion == version) {
                if (STATE.compareAndSet(this, REQUESTED, TERMINATED)) {
                    actual.onNext(owner.device);
                    actual.onComplete();
                }
            } else {
                owner.validation(version).subscribe(this);
            }
        }

        @Override
        public void cancel() {
            int previous;
            do {
                previous = state;
                if (previous == CANCELLED || previous == TERMINATED) {
                    return;
                }
            } while (!STATE.compareAndSet(this, previous, CANCELLED));
            Subscription current = fallback;
            if (current != null) {
                current.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            fallback = subscription;
            if (state == CANCELLED) {
                subscription.cancel();
            } else {
                subscription.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(DeviceOperator value) {
            if (state == REQUESTED) {
                actual.onNext(value);
            } else {
                Operators.onDiscard(value, currentContext());
            }
        }

        @Override
        public void onError(Throwable error) {
            if (STATE.compareAndSet(this, REQUESTED, TERMINATED)) {
                actual.onError(error);
            } else {
                Operators.onErrorDropped(error, currentContext());
            }
        }

        @Override
        public void onComplete() {
            if (STATE.compareAndSet(this, REQUESTED, TERMINATED)) {
                actual.onComplete();
            }
        }

        @Override
        @Nonnull
        public reactor.util.context.Context currentContext() {
            return actual.currentContext();
        }
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
