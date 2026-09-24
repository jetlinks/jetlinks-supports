package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import org.jetlinks.core.device.DeviceOperator;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * 首次订阅校验设备存在性，缓存有效期间直接返回设备操作对象。
 */
final class MonoValidatedDeviceOperator extends Mono<DeviceOperator> implements Scannable {

    private static final Object CACHE_DELEGATION_KEY = new Object();

    private static final AtomicLongFieldUpdater<MonoValidatedDeviceOperator> INVALIDATION_VERSION =
        AtomicLongFieldUpdater.newUpdater(MonoValidatedDeviceOperator.class, "invalidationVersion");

    private static final AtomicLongFieldUpdater<MonoValidatedDeviceOperator> VALIDATED_VERSION =
        AtomicLongFieldUpdater.newUpdater(MonoValidatedDeviceOperator.class, "validatedVersion");

    private final String deviceId;
    private final DeviceOperator device;
    private final Mono<?> validator;
    private static final AtomicLongFieldUpdater<MonoValidatedDeviceOperator> LAST_ACCESS =
        AtomicLongFieldUpdater.newUpdater(MonoValidatedDeviceOperator.class, "lastAccessNanos");

    private final Object cache;

    private volatile long invalidationVersion;
    private volatile long validatedVersion = -1;
    private volatile long lastAccessNanos;

    MonoValidatedDeviceOperator(String deviceId,
                                DeviceOperator device,
                                Mono<?> validator,
                                Cache<String, Mono<DeviceOperator>> cache) {
        this(deviceId, device, validator, (Object) cache);
    }

    MonoValidatedDeviceOperator(String deviceId,
                                DeviceOperator device,
                                Mono<?> validator,
                                ConcurrentValidatedDeviceCache cache) {
        this(deviceId, device, validator, (Object) cache);
    }

    private MonoValidatedDeviceOperator(String deviceId,
                                        DeviceOperator device,
                                        Mono<?> validator,
                                        Object cache) {
        this.deviceId = Objects.requireNonNull(deviceId, "deviceId");
        this.device = Objects.requireNonNull(device, "device");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.cache = Objects.requireNonNull(cache, "cache");
    }

    @Override
    public void subscribe(@Nonnull CoreSubscriber<? super DeviceOperator> actual) {
        CacheDelegation delegation = actual.currentContext().getOrDefault(CACHE_DELEGATION_KEY, null);
        if (delegation != null && delegation.matches(cache, deviceId)) {
            // 包装操作符保留原订阅语义，只在最终到达目标实例时停止再次追逐缓存。
            subscribeResolved(new CacheResolvedSubscriber(actual, delegation.parent));
            return;
        }
        Mono<DeviceOperator> cached = getCached();
        if (cached != this) {
            if (cached == null) {
                cached = putIfAbsent();
            }
            if (cached != null && cached != this) {
                subscribeCached(cached, actual);
                return;
            }
        }

        subscribeResolved(actual);
    }

    private void subscribeCached(Mono<DeviceOperator> cached,
                                 CoreSubscriber<? super DeviceOperator> actual) {
        CacheDelegation parent = actual.currentContext().getOrDefault(CACHE_DELEGATION_KEY, null);
        cached.subscribe(new CacheDelegatingSubscriber(
            actual,
            new CacheDelegation(cache, deviceId, parent)
        ));
    }

    private void subscribeResolved(CoreSubscriber<? super DeviceOperator> actual) {
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
                if (invalidationVersion == version && getCached() == this) {
                    VALIDATED_VERSION.set(this, version);
                }
            })
            .doOnSuccess(value -> {
                if (value == null) {
                    removeCached();
                }
            });
    }

    @SuppressWarnings("unchecked")
    private Mono<DeviceOperator> getCached() {
        if (cache instanceof ConcurrentValidatedDeviceCache) {
            return ((ConcurrentValidatedDeviceCache) cache).get(deviceId);
        }
        return ((Cache<String, Mono<DeviceOperator>>) cache).getIfPresent(deviceId);
    }

    @SuppressWarnings("unchecked")
    private Mono<DeviceOperator> putIfAbsent() {
        if (cache instanceof ConcurrentValidatedDeviceCache) {
            return ((ConcurrentValidatedDeviceCache) cache).putIfAbsent(deviceId, this);
        }
        return ((Cache<String, Mono<DeviceOperator>>) cache).asMap().putIfAbsent(deviceId, this);
    }

    @SuppressWarnings("unchecked")
    private void removeCached() {
        if (cache instanceof ConcurrentValidatedDeviceCache) {
            ((ConcurrentValidatedDeviceCache) cache).remove(deviceId, this);
        } else {
            ((Cache<String, Mono<DeviceOperator>>) cache).asMap().remove(deviceId, this);
        }
    }

    void touch(long now, long sampleNanos) {
        long previous = lastAccessNanos;
        if (previous == 0 || now - previous >= sampleNanos) {
            LAST_ACCESS.compareAndSet(this, previous, now);
        }
    }

    boolean isIdle(long now, long expireAfterAccessNanos) {
        return now - lastAccessNanos >= expireAfterAccessNanos;
    }

    private static final class CacheDelegatingSubscriber implements CoreSubscriber<DeviceOperator> {

        private final CoreSubscriber<? super DeviceOperator> actual;
        private final CacheDelegation delegation;

        private CacheDelegatingSubscriber(CoreSubscriber<? super DeviceOperator> actual,
                                          CacheDelegation delegation) {
            this.actual = actual;
            this.delegation = delegation;
        }

        @Override
        public void onSubscribe(@Nonnull Subscription subscription) {
            actual.onSubscribe(subscription);
        }

        @Override
        public void onNext(DeviceOperator device) {
            actual.onNext(device);
        }

        @Override
        public void onError(Throwable error) {
            actual.onError(error);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

        @Override
        @Nonnull
        public Context currentContext() {
            return actual.currentContext().put(CACHE_DELEGATION_KEY, delegation);
        }
    }

    private static final class CacheResolvedSubscriber implements CoreSubscriber<DeviceOperator> {

        private final CoreSubscriber<? super DeviceOperator> actual;
        private final CacheDelegation parent;

        private CacheResolvedSubscriber(CoreSubscriber<? super DeviceOperator> actual,
                                        CacheDelegation parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void onSubscribe(@Nonnull Subscription subscription) {
            actual.onSubscribe(subscription);
        }

        @Override
        public void onNext(DeviceOperator device) {
            actual.onNext(device);
        }

        @Override
        public void onError(Throwable error) {
            actual.onError(error);
        }

        @Override
        public void onComplete() {
            actual.onComplete();
        }

        @Override
        @Nonnull
        public Context currentContext() {
            Context context = actual.currentContext();
            return parent == null
                ? context.delete(CACHE_DELEGATION_KEY)
                : context.put(CACHE_DELEGATION_KEY, parent);
        }
    }

    private static final class CacheDelegation {

        private final Object cache;
        private final String deviceId;
        private final CacheDelegation parent;

        private CacheDelegation(Object cache,
                                String deviceId,
                                CacheDelegation parent) {
            this.cache = cache;
            this.deviceId = deviceId;
            this.parent = parent;
        }

        private boolean matches(Object cache, String deviceId) {
            return this.cache == cache && this.deviceId.equals(deviceId);
        }
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
            // 已通过检查的并发请求允许旧结果；不要在下游回调期间锁住同一设备。
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
