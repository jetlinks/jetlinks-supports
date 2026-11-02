package org.jetlinks.supports.config;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.config.ConfigKey;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.utils.Reactors;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Manager-owned L1 values with shared single-key L2 fills and independent batch/default
 * subscriptions. Invalidated entries can finish old readers but cannot populate replacements.
 * Shared fills continue after reader cancellation; their deadlines belong to the source.
 * Hits perform no observation work. The manager owns notification and observation resources,
 * not the lifetime of already-started reads.
 *
 * @see ConfigStorage
 * @see EventBusStorageManager
 */
@Slf4j
public class LocalCacheClusterConfigStorage implements ConfigStorage {
    private static final Map<Value, Value> shared = new ConcurrentReferenceHashMap<>(1024);
    private static final Set<String> sharedKey = ConcurrentHashMap.newKeySet();
    public static final Value NULL = Value.simple(null);
    private static final Values EMPTY_VALUES = Values.of(Collections.emptyMap());
    private static final Mono<Values> EMPTY_VALUES_MONO = Mono.just(EMPTY_VALUES);
    private static final Snapshot EMPTY = new Snapshot(0, null);
    private static volatile CompositeType inspectionType;

    private final Map<String, Cache> caches;
    final String id;
    private final EventBusStorageManager manager;
    private final ClusterCache<String, Object> clusterCache;
    private final long expiresNanos;
    private final Consumer<LocalCacheClusterConfigStorage> doOnClear;
    // Bounded error identity for the legacy search view; never retain a Throwable graph.
    volatile String lastError;

    LocalCacheClusterConfigStorage(String id, EventBusStorageManager manager,
                                   ClusterCache<String, Object> clusterCache, long expires,
                                   Consumer<LocalCacheClusterConfigStorage> doOnClear,
                                   Map<String, Cache> cacheContainer) {
        this.id = id;
        this.manager = manager;
        this.clusterCache = clusterCache;
        this.expiresNanos = TimeUnit.MILLISECONDS.toNanos(expires);
        this.doOnClear = doOnClear;
        this.caches = cacheContainer;
    }

    LocalCacheClusterConfigStorage(String id, EventBusStorageManager manager,
                                   ClusterCache<String, Object> clusterCache, long expires,
                                   Consumer<LocalCacheClusterConfigStorage> doOnClear) {
        this(id, manager, clusterCache, expires, doOnClear, new ConcurrentHashMap<>());
    }

    boolean isEmpty() { return caches.isEmpty(); }
    int localEntryCount() { return caches.size(); }

    CompositeData[] inspect(int limit) {
        List<CompositeData> result = new ArrayList<>(Math.min(limit, caches.size()));
        CompositeType type = inspectionType;
        for (Map.Entry<String, Cache> entry : caches.entrySet()) {
            Cache cache = entry.getValue();
            Snapshot state = cache.current;
            String key = entry.getKey();
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("key", key.length() <= 512 ? key : key.substring(0, 512));
            fields.put("keyTruncated", key.length() > 512);
            fields.put("state", cache.isExpired() ? "expired" : cache.loading != null ? "loading"
                : state.value == null ? "empty" : state.value == NULL ? "negative" : "value");
            fields.put("version", (long) state.version);
            if (type == null) inspectionType = type = ConfigStorageMonitor.createType("LocalConfigEntry", fields);
            result.add(ConfigStorageMonitor.openData(type, fields));
            if (result.size() == limit) break;
        }
        return result.toArray(new CompositeData[0]);
    }

    void cleanup() {
        if (expiresNanos > 0) caches.forEach((key, cache) -> {
            if (cache.isExpired() && cache.loading == null) caches.remove(key, cache);
        });
    }

    void error(Throwable error) {
        lastError = error.getClass().getName();
        manager.monitor.loadError(error);
    }

    private Cache createCache(String key) { return new Cache(key); }

    private Cache findCache(String key) {
        Cache found = caches.get(key);
        if (found != null) return found;
        LocalCacheClusterConfigStorage active = manager.cache.get(id);
        return active == null || active == this ? null : active.caches.get(key);
    }

    private Value cachedValue(String key) {
        Cache cache = findCache(key);
        return cache == null ? null : cache.getCached();
    }

    private Cache getOrCreateCache(String key) {
        Cache found = caches.get(key);
        if (found != null) return found;
        // Pair cold registration with empty removal, so an admitted key retains
        // its manager registration and therefore still receives invalidations.
        for (;;) {
            LocalCacheClusterConfigStorage active = manager.cache.get(id);
            if (active == null) active = this;
            synchronized (active) {
                LocalCacheClusterConfigStorage registered = manager.cache.get(id);
                if (registered == null) registered = manager.cache.putIfAbsent(id, active);
                if (registered != null && registered != active) continue;
                // Close may clear the manager while this cold registration waits.
                // Do not reattach an entry after its owner has released its resources.
                if (manager.isDisposed()) {
                    manager.cache.remove(id, active);
                    throw new CancellationException("Storage manager is disposed");
                }
                return active.caches.computeIfAbsent(key, active::createCache);
            }
        }
    }

    @Override
    public Mono<Value> getConfig(String key) { return getConfig(key, null); }

    @Override
    public Mono<Value> getConfig(String key, Mono<Object> fallback) {
        Cache cache = findCache(key);
        Snapshot state = cache == null ? null : cache.cachedState();
        if (state != null && state.value != NULL) return state.result;
        if (fallback != null) return Mono.deferContextual(context -> {
            if (manager.isDisposed()) return Mono.error(new CancellationException("Storage manager is disposed"));
            Cache entry = getOrCreateCache(key);
            return entry.read(Context.of(context)).switchIfEmpty(Mono.defer(() -> fallbackRead(entry, fallback)));
        });
        // Every miss subscription, including retry, claims the current load again.
        return state == null ? Mono.deferContextual(context -> {
            if (manager.isDisposed()) return Mono.error(new CancellationException("Storage manager is disposed"));
            return getOrCreateCache(key).read(Context.of(context));
        }) : state.result;
    }

    private Mono<Value> fallbackRead(Cache cache, Mono<Object> fallback) {
        if (manager.isDisposed()) return Mono.error(new CancellationException("Storage manager is disposed"));
        Snapshot start = cache.current;
        if (start.value != null && start.value != NULL) return start.result;
        // L2 may have selected an expired negative as stale fallback. Do not reload
        // that negative here: each caller now owns only its default subscription.
        return fallback.map(value -> {
            Value selected = Value.simple(value);
            // Keep the entry selected by this subscription even if invalidation
            // replaced it between the negative L2 signal and this default.
            cache.store(start, selected);
            return selected;
        });
    }

    @Override
    public Mono<Values> getConfigs(Collection<String> keys) {
        Mono<Values> cached = getCachedValues(keys);
        if (cached != null) return cached;
        List<String> requested = new ArrayList<>(keys);
        // MonoSink supplies demand buffering and cancellation for this one batch.
        return Mono.create(sink -> new BatchLoader(sink, requested.size()).start(requested));
    }

    private Mono<Values> getCachedValues(Collection<String> keys) {
        if (keys.isEmpty()) return EMPTY_VALUES_MONO;
        int count = keys.size();
        if (count == 2 || count == 3) {
            Iterator<String> it = keys.iterator();
            String firstKey = it.next();
            Value first = cachedValue(firstKey);
            if (first == null) return null;
            String secondKey = it.next();
            Value second = cachedValue(secondKey);
            if (second == null) return null;
            if (count == 2) return first.get() == null && second.get() == null ? EMPTY_VALUES_MONO
                : Mono.just(Values.of(firstKey, first.get(), secondKey, second.get()));
            String thirdKey = it.next();
            Value third = cachedValue(thirdKey);
            if (third == null) return null;
            return first.get() == null && second.get() == null && third.get() == null ? EMPTY_VALUES_MONO
                : Mono.just(Values.of(firstKey, first.get(), secondKey, second.get(), thirdKey, third.get()));
        }
        Map<String, Object> values = null;
        for (String key : keys) {
            Value value = cachedValue(key);
            if (value == null) return null;
            if (value.get() != null) {
                if (values == null) values = Maps.newHashMapWithExpectedSize(count);
                values.put(key, value.get());
            }
        }
        return values == null ? EMPTY_VALUES_MONO : Mono.just(Values.of(values));
    }

    @Override
    public Mono<Boolean> setConfigs(Map<String, Object> values) {
        if (CollectionUtils.isEmpty(values)) return Reactors.ALWAYS_TRUE;
        return clusterCache.putAll(values)
            .doOnSuccess(ignored -> values.forEach(this::trySetLocalValue))
            .then(notify(CacheNotify.expires(id, values.keySet())))
            .then(Reactors.ALWAYS_TRUE);
    }

    @Override
    public Mono<Boolean> setConfig(String key, Object value) {
        if (key == null) return Reactors.ALWAYS_FALSE;
        if (value == null) return remove(key);
        return clusterCache.put(key, value)
            .doOnSuccess(ignored -> trySetLocalValue(key, value))
            .then(notifyRemoveKey(key)).then(Reactors.ALWAYS_TRUE);
    }

    private void trySetLocalValue(String key, Object value) {
        LocalCacheClusterConfigStorage active = manager.cache.get(id);
        Cache cache = (active == null ? this : active).caches.get(key);
        if (cache != null) cache.publish(value == null ? NULL : tryShare(key, Value.simple(value)));
    }

    @Override public Mono<Boolean> remove(String key) {
        return clusterCache.remove(key).then(notifyRemoveKey(key)).then(Reactors.ALWAYS_TRUE);
    }

    @Override public Mono<Value> getAndRemove(String key) {
        return clusterCache.getAndRemove(key)
            .flatMap(value -> notifyRemoveKey(key).thenReturn(Value.simple(value)))
            .switchIfEmpty(notifyRemoveKey(key).then(Mono.empty()));
    }

    @Override public Mono<Boolean> remove(Collection<String> keys) {
        return clusterCache.remove(keys).then(notify(CacheNotify.expires(id, keys))).then(Reactors.ALWAYS_TRUE);
    }

    @Override public Mono<Boolean> clear() {
        return clusterCache.clear().then(notify(CacheNotify.clear(id))).then(Reactors.ALWAYS_TRUE);
    }

    void clearLocalCache(CacheNotify notify) {
        if (CollectionUtils.isEmpty(notify.getKeys())) caches.clear();
        else for (String key : notify.getKeys()) caches.remove(key);
        if (notify.isClear() && doOnClear != null) doOnClear.accept(this);
    }

    Mono<Void> notify(CacheNotify notify) {
        return Mono.defer(() -> {
            LocalCacheClusterConfigStorage active = manager.cache.get(id);
            if (active != null && active != this) active.clearLocalCache(notify);
            clearLocalCache(notify);
            return manager.doNotify(notify);
        });
    }

    Mono<Void> notifyRemoveKey(String key) { return notify(CacheNotify.expires(id, Collections.singleton(key))); }
    Map<String, Object> getAll() { return Collections.unmodifiableMap(Maps.transformValues(caches, Cache::getCachedValue)); }
    @Override public Mono<Void> refresh() { return notify(CacheNotify.expiresAll(id)); }
    @Override public Mono<Void> refresh(Collection<String> keys) { return notify(CacheNotify.expires(id, keys)); }

    static {
        addSharedKey(DeviceConfigKey.productId, DeviceConfigKey.protocol, DeviceConfigKey.connectionServerId);
        addSharedKey("state", "productName");
    }
    public static void addSharedKey(ConfigKey<?>... keys) { for (ConfigKey<?> key : keys) sharedKey.add(key.getKey()); }
    public static void addSharedKey(String... keys) { sharedKey.addAll(Arrays.asList(keys)); }
    private static Value tryShare(String key, Value value) {
        return sharedKey.contains(key) ? shared.computeIfAbsent(value, Function.identity()) : value;
    }

    /** Value, reusable hit publisher and version are a single volatile publication. */
    private static final class Snapshot {
        final int version;
        final Value value;
        final Mono<Value> result;
        Snapshot(int version, Value value) {
            this.version = version;
            this.value = value;
            result = value == null || value == NULL ? Mono.empty() : Mono.just(value);
        }
    }

    /** L1 publication and shared single-load ownership use only this entry's lock. */
    public class Cache {
        final String key;
        volatile long lastAccessNanos;
        private volatile Snapshot current = EMPTY;
        private volatile SingleLoader loading;
        public Cache(String key) { this.key = key; touch(); }
        int version() { return current.version; }
        boolean isExpired() { return expiresNanos > 0 && System.nanoTime() - lastAccessNanos > expiresNanos; }
        void touch() { if (expiresNanos > 0) lastAccessNanos = System.nanoTime(); }
        Snapshot cachedState() {
            Snapshot state = current;
            if (state.value == null) return null;
            if (expiresNanos > 0) {
                long now = System.nanoTime();
                if (now - lastAccessNanos > expiresNanos) return null;
                lastAccessNanos = now;
            }
            return state;
        }
        public Value getCached() { Snapshot state = cachedState(); return state == null ? null : state.value; }
        public Object getCachedValue() { touch(); Value value = current.value; return value == null ? null : value.get(); }

        Mono<Value> read(Context context) {
            SingleLoader load;
            synchronized (this) {
                Snapshot state = cachedState();
                if (state != null) return state.result;
                if (loading != null) return loading.result;
                loading = load = new SingleLoader(this, current, context);
            }
            // A shared fill is started once, with its first caller's Context. A
            // reader cancelling its replay subscription does not cancel the fill.
            try { if (!load.isDisposed()) clusterCache.get(key).subscribe(load); }
            catch (Throwable failure) { Exceptions.throwIfFatal(failure); fail(load, failure); }
            return load.result;
        }

        void publish(Value value) {
            SingleLoader pending;
            synchronized (this) {
                current = new Snapshot(current.version + 1, value);
                pending = loading;
                loading = null;
                touch();
            }
            if (pending != null) pending.deliver(value);
        }

        Value store(Snapshot expected, Value value) {
            Value selected = value == NULL ? NULL : tryShare(key, value);
            synchronized (this) {
                if (current == expected) {
                    current = new Snapshot(current.version + 1, selected);
                    touch();
                } else if (current.value != null) selected = current.value;
            }
            return selected;
        }

        void complete(SingleLoader load, Value value) {
            Value selected = value == NULL ? NULL : tryShare(key, value);
            synchronized (this) {
                if (loading != load) return;
                if (current == load.start) {
                    current = new Snapshot(current.version + 1, selected);
                    touch();
                } else if (current.value != null) selected = current.value;
                // Clear before signalling: a reentrant retry must see no old slot.
                loading = null;
            }
            load.deliver(selected);
        }

        void fail(SingleLoader load, Throwable failure) {
            Value stale;
            synchronized (this) {
                if (loading != load) return;
                loading = null;
                stale = failure instanceof CancellationException ? null : current.value;
            }
            if (!(failure instanceof CancellationException)) error(failure);
            if (stale == null) load.signal.tryEmitError(failure);
            else {
                manager.monitor.staleFallbacks.increment();
                log.warn("load cache [{} {}] failed; using a previous value, version:{}", id, key, version(), failure);
                load.deliver(stale);
            }
        }

        @Override public String toString() { return key + ":" + version(); }
    }

    /** One source and replay result per shared fill; no per-reader lifecycle state. */
    private final class SingleLoader extends BaseSubscriber<Object> {
        final Cache cache;
        final Snapshot start;
        final Context context;
        final Sinks.One<Value> signal = Sinks.one();
        final Mono<Value> result = signal.asMono();
        SingleLoader(Cache cache, Snapshot start, Context context) {
            this.cache = cache;
            this.start = start;
            this.context = context;
        }
        @Override public Context currentContext() { return context; }
        @Override protected void hookOnNext(Object value) { finish(Value.simple(value)); }
        @Override protected void hookOnComplete() { finish(NULL); }
        private void finish(Value value) {
            try { cache.complete(this, value); }
            catch (Throwable failure) { Exceptions.throwIfFatal(failure); cache.fail(this, failure); }
        }
        @Override protected void hookOnError(Throwable failure) { cache.fail(this, failure); }
        void deliver(Value value) {
            cancel();
            if (value == NULL) signal.tryEmitEmpty();
            else signal.tryEmitValue(value);
        }
    }

    /** A batch retains only publication guards; it never registers key waiters. */
    private static final class BatchTarget {
        final Cache cache;
        final Snapshot start;
        BatchTarget(Cache cache, Snapshot start) { this.cache = cache; this.start = start; }
    }

    /** The MonoSink owns downstream demand/terminal delivery; this owns one L2 subscription. */
    private final class BatchLoader extends BaseSubscriber<Map.Entry<String, Object>> {
        final MonoSink<Values> sink;
        final Context context;
        final Map<String, BatchTarget> targets;
        final Map<String, Object> values;
        BatchLoader(MonoSink<Values> sink, int size) {
            this.sink = sink;
            context = Context.of(sink.contextView());
            targets = Maps.newHashMapWithExpectedSize(size);
            values = Maps.newHashMapWithExpectedSize(size);
        }
        void start(List<String> keys) {
            sink.onCancel(this);
            if (isDisposed()) return;
            if (manager.isDisposed()) { sink.error(new CancellationException("Storage manager is disposed")); return; }
            try {
                for (String key : keys) {
                    if (isDisposed()) return;
                    if (targets.containsKey(key)) continue;
                    Cache cache = getOrCreateCache(key);
                    // Capture before deciding a miss: a concurrent publication must
                    // never become the expected generation for an older L2 result.
                    Snapshot start = cache.current;
                    Snapshot state = cache.cachedState();
                    if (state == null) {
                        // A repeated key can lose an earlier L1 seed to invalidation.
                        values.remove(key);
                        targets.put(key, new BatchTarget(cache, start));
                    }
                    else if (state.value != NULL) values.put(key, state.value.get());
                }
                if (isDisposed()) return;
                if (targets.isEmpty()) sink.success(resultValues());
                else clusterCache.get(Collections.unmodifiableSet(targets.keySet())).subscribe(this);
            } catch (Throwable failure) {
                Exceptions.throwIfFatal(failure);
                if (!isDisposed()) { cancel(); hookOnError(failure); }
            }
        }
        @Override public Context currentContext() { return context; }
        @Override protected void hookOnNext(Map.Entry<String, Object> value) {
            if (targets.containsKey(value.getKey()))
                values.putIfAbsent(value.getKey(), value.getValue() == null ? NULL : value.getValue());
        }
        @Override protected void hookOnComplete() {
            try {
                for (Map.Entry<String, BatchTarget> target : targets.entrySet()) {
                    Object received = values.get(target.getKey());
                    Value selected = received == null || received == NULL ? NULL : Value.simple(received);
                    BatchTarget expected = target.getValue();
                    Object value = expected.cache.store(expected.start, selected).get();
                    if (value == null) values.remove(target.getKey());
                    else values.put(target.getKey(), value);
                }
                sink.success(resultValues());
            } catch (Throwable failure) {
                Exceptions.throwIfFatal(failure);
                hookOnError(failure);
            }
        }
        private Values resultValues() { return values.isEmpty() ? EMPTY_VALUES : Values.of(values); }
        @Override protected void hookOnError(Throwable failure) {
            if (!(failure instanceof CancellationException)) error(failure);
            sink.error(failure);
        }
    }

    @Override public String toString() { return caches.toString(); }
}
