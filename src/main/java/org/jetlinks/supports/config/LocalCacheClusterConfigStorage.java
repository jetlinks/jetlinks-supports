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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import reactor.core.Disposable;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Manager-owned L1 configuration values. A key publishes its value and version together;
 * single-key subscribers share a load; each batch owns its request and result map.
 * Invalidated entries may finish old readers but cannot populate a replacement entry.
 * Deadlines belong to the underlying source. Cancellation and shutdown release owned work;
 * hits perform no observation work.
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
    private final long expires;
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
        this.expires = expires;
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
            fields.put("state", cache.isExpired() ? "expired" : cache.loading != null || cache.batches != null ? "loading"
                : state.value == null ? "empty" : state.value == NULL ? "negative" : "value");
            fields.put("version", (long) state.version);
            if (type == null) inspectionType = type = ConfigStorageMonitor.createType("LocalConfigEntry", fields);
            result.add(ConfigStorageMonitor.openData(type, fields));
            if (result.size() == limit) break;
        }
        return result.toArray(new CompositeData[0]);
    }

    void cleanup() {
        if (expires > 0) caches.forEach((key, cache) -> {
            if (cache.isExpired() && cache.loading == null && cache.batches == null) caches.remove(key, cache);
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
        // Only cold registration and empty removal share this storage's lock.
        // Registered storages do not mutate the manager map for each new key.
        for (;;) {
            LocalCacheClusterConfigStorage active = manager.cache.get(id);
            if (active == null) active = this;
            synchronized (active) {
                LocalCacheClusterConfigStorage registered = manager.cache.get(id);
                if (registered == null) registered = manager.cache.putIfAbsent(id, active);
                if (registered != null && registered != active) continue;
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
        // Resolve again per subscription: retry must claim new work, never replay a failed sink.
        Mono<Value> read = state == null ? Mono.defer(() -> singleRead(key)) : state.result;
        return fallback == null ? read : read.switchIfEmpty(Mono.defer(() -> fallbackRead(key, fallback)));
    }

    private Mono<Value> singleRead(String key) {
        Cache cache = getOrCreateCache(key);
        Snapshot state = cache.cachedState();
        if (state != null) return state.result;
        SingleLoader load = cache.acquire();
        return load == null ? cache.current.result : load.result;
    }

    private Mono<Value> fallbackRead(String key, Mono<Object> fallback) {
        Cache cache = getOrCreateCache(key);
        Snapshot state = cache.current;
        if (state.value == null) {
            // Invalidation can replace the negative generation between the L2 result
            // and fallback subscription. Resolve that generation before running a default.
            return singleRead(key).switchIfEmpty(Mono.defer(() -> fallbackRead(key, fallback)));
        }
        // The shared read can intentionally return a stale negative on L2 failure.
        // Rechecking its idle expiry here would retry that failed L2 forever instead
        // of invoking this caller's default. A replacement EMPTY generation still reloads.
        if (state.value != NULL) return state.result;
        FallbackLoad load;
        synchronized (cache) {
            if (cache.current != state) return Mono.defer(() -> fallbackRead(key, fallback));
            load = new FallbackLoad(cache, state);
            if (!manager.startLoad(id, load)) {
                load.dispose();
            } else {
                if (cache.fallbacks == null) cache.fallbacks = new LinkedHashSet<>();
                cache.fallbacks.add(load);
            }
        }
        return load.run(fallback);
    }

    @Override
    public Mono<Values> getConfigs(Collection<String> keys) {
        Mono<Values> cached = getCachedValues(keys);
        if (cached != null) return cached;
        // Copy only the keys, not mutable targets/results, across subscriptions.
        List<String> requested = new ArrayList<>(keys);
        return Mono.defer(() -> Mono.fromDirect(new BatchLoader(requested)));
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
            .switchIfEmpty(Mono.defer(() -> notifyRemoveKey(key).then(Mono.empty())));
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

    /**
     * Storage-owned L1 entry. Its lock protects state publication and waiter
     * registration; the loaders own remote subscriptions and cancellation.
     */
    public class Cache {
        final String key;
        volatile long t;
        private volatile Snapshot current = EMPTY;
        private volatile SingleLoader loading;
        private volatile BatchEntry batches;
        private Set<FallbackLoad> fallbacks;
        public Cache(String key) { this.key = key; touch(); }
        int version() { return current.version; }
        boolean isExpired() { return expires > 0 && System.currentTimeMillis() - t > expires; }
        void touch() { if (expires > 0) t = System.currentTimeMillis(); }
        Snapshot cachedState() {
            Snapshot state = current;
            if (state.value == null || isExpired()) return null;
            touch();
            return state;
        }
        public Value getCached() { Snapshot state = cachedState(); return state == null ? null : state.value; }
        public Object getCachedValue() { touch(); Value value = current.value; return value == null ? null : value.get(); }

        void publish(Value value) {
            SingleLoader pending;
            BatchEntry waiting;
            Set<FallbackLoad> defaults;
            synchronized (this) {
                current = new Snapshot(current.version + 1, value);
                pending = loading;
                loading = null;
                if (pending != null) pending.outcome = value;
                waiting = resolveBatches(value);
                defaults = resolveFallbacks(value);
                touch();
            }
            if (pending != null) pending.deliver();
            deliverBatches(waiting);
            deliverFallbacks(defaults, null);
        }

        SingleLoader acquire() {
            synchronized (this) {
                Snapshot state = current;
                if (state.value != null && !isExpired()) { touch(); return null; }
                if (loading != null) { loading.readers++; return loading; }
                SingleLoader load = new SingleLoader(this, state);
                if (!manager.startLoad(id, load)) load.dispose();
                if (load.outcome == null) loading = load;
                return load;
            }
        }

        void complete(SingleLoader pending, Value value) {
            // Sharing can invoke user hashCode. Recheck ownership after it, then publish
            // the value/version and reserve the waiter's outcome in one critical section.
            Value selected = value == null ? NULL : tryShare(key, value);
            Set<FallbackLoad> defaults = null;
            synchronized (this) {
                if (pending.outcome == null) {
                    if (loading == pending && current == pending.start) {
                        current = new Snapshot(current.version + 1, selected);
                        if (selected != NULL) defaults = resolveFallbacks(selected);
                        touch();
                    } else if (current.value != null) selected = current.value;
                    if (loading == pending) loading = null;
                    pending.outcome = selected;
                }
            }
            pending.deliver();
            deliverFallbacks(defaults, null);
        }

        void fail(SingleLoader pending, Throwable error) {
            boolean fallback = false;
            boolean report = false;
            synchronized (this) {
                if (pending.outcome == null) {
                    if (loading == pending) loading = null;
                    Value stale = current.value;
                    fallback = stale != null && !(error instanceof CancellationException);
                    pending.outcome = fallback ? stale : error;
                    report = !(error instanceof CancellationException);
                }
            }
            if (report) error(error);
            if (fallback) {
                manager.monitor.staleFallbacks.increment();
                log.warn("load cache [{} {}] failed; using a previous value, version:{}", id, key, version(), error);
            }
            pending.deliver();
        }

        // Batches do not share upstream work. These entries exist only so a committed
        // write can end their wait without waiting for an unrelated backend response.
        private BatchEntry resolveBatches(Value value) {
            BatchEntry waiting = batches;
            batches = null;
            for (BatchEntry entry = waiting; entry != null; entry = entry.next) entry.outcome = value;
            return waiting;
        }

        private void removeBatch(BatchEntry entry) {
            BatchEntry previous = null;
            for (BatchEntry current = batches; current != null; current = current.next) {
                if (current == entry) {
                    if (previous == null) batches = current.next;
                    else previous.next = current.next;
                    current.next = null;
                    return;
                }
                previous = current;
            }
        }

        void complete(BatchEntry entry, Value value) {
            Value selected = value == null ? NULL : tryShare(key, value);
            Set<FallbackLoad> defaults = null;
            synchronized (this) {
                if (entry.outcome == null) {
                    if (current == entry.start) {
                        current = new Snapshot(current.version + 1, selected);
                        if (selected != NULL) defaults = resolveFallbacks(selected);
                        touch();
                    } else if (current.value != null) selected = current.value;
                    removeBatch(entry);
                    entry.outcome = selected;
                }
            }
            deliverFallbacks(defaults, null);
        }

        void fail(BatchEntry entry, Throwable failure) {
            boolean stale = false;
            synchronized (this) {
                if (entry.outcome == null) {
                    stale = current.value != null && !(failure instanceof CancellationException);
                    removeBatch(entry);
                    entry.outcome = stale ? current.value : failure;
                }
            }
            if (stale) manager.monitor.staleFallbacks.increment();
        }

        // Called only under this cache's lock, so a write reserves every waiter's
        // result before an old source can publish or fail it.
        private Set<FallbackLoad> resolveFallbacks(Value value) {
            Set<FallbackLoad> defaults = fallbacks;
            fallbacks = null;
            if (defaults != null) for (FallbackLoad load : defaults) load.outcome = value;
            return defaults;
        }

        private void removeFallback(FallbackLoad load) {
            if (fallbacks != null) {
                fallbacks.remove(load);
                if (fallbacks.isEmpty()) fallbacks = null;
            }
        }

        void completeFallback(FallbackLoad load, Object value) {
            Value selected = value == null ? NULL : tryShare(key, Value.simple(value));
            SingleLoader pending = null;
            BatchEntry waiting = null;
            Set<FallbackLoad> defaults = null;
            synchronized (this) {
                if (load.outcome == null) {
                    if (selected != NULL && current == load.start) {
                        current = new Snapshot(current.version + 1, selected);
                        touch();
                        pending = loading;
                        loading = null;
                        if (pending != null) pending.outcome = selected;
                        waiting = resolveBatches(selected);
                        defaults = resolveFallbacks(selected);
                    } else {
                        removeFallback(load);
                        load.outcome = current.value != null && current.value != NULL ? current.value : selected;
                    }
                }
            }
            if (pending != null) pending.deliver();
            deliverBatches(waiting);
            deliverFallbacks(defaults, load);
            load.deliver();
        }

        void failFallback(FallbackLoad load, Throwable failure) {
            boolean report = false;
            synchronized (this) {
                if (load.outcome == null) {
                    removeFallback(load);
                    load.outcome = failure;
                    report = !(failure instanceof CancellationException);
                }
            }
            if (report) error(failure);
            load.deliver();
        }

        void releaseFallback(FallbackLoad load) {
            synchronized (this) {
                removeFallback(load);
                if (load.outcome == null) load.outcome = new CancellationException("Configuration reader cancelled");
            }
            manager.endLoad(id, load);
        }
        @Override public String toString() { return key + ":" + version(); }
    }

    private void deliverFallbacks(Set<FallbackLoad> defaults, FallbackLoad except) {
        if (defaults != null) for (FallbackLoad load : defaults) if (load != except) load.deliver();
    }

    private void deliverBatches(BatchEntry waiting) {
        for (BatchEntry entry = waiting; entry != null; entry = entry.next) entry.owner.finishIfReady();
    }

    /** A default belongs to one subscriber; ordinary readers only share L2 work. */
    private final class FallbackLoad implements Disposable {
        final Cache cache;
        final Snapshot start;
        final Sinks.One<Value> changed = Sinks.one();
        volatile Object outcome;
        FallbackLoad(Cache cache, Snapshot start) { this.cache = cache; this.start = start; }
        Mono<Value> run(Mono<Object> fallback) {
            // Both source completion and writes reserve and deliver the same outcome.
            // Keep shutdown registration until delivery, including the empty/error case.
            Mono<Value> source = fallback.doOnSuccess(value -> cache.completeFallback(this, value))
                .onErrorResume(this::failed).then(changed.asMono());
            return Mono.firstWithSignal(changed.asMono(), source)
                .filter(value -> value != NULL)
                .doFinally(ignored -> cache.releaseFallback(this));
        }
        Mono<Object> failed(Throwable failure) {
            cache.failFallback(this, failure);
            return Mono.empty();
        }
        void deliver() {
            Object terminal = outcome;
            if (terminal instanceof Value) changed.tryEmitValue((Value) terminal);
            else changed.tryEmitError((Throwable) terminal);
            manager.endLoad(id, this);
        }
        @Override public void dispose() {
            cache.failFallback(this, new CancellationException("Storage manager is disposed"));
        }
        @Override public boolean isDisposed() { return outcome != null; }
    }

    /** A shared single-key source and its result have one owner and one cache lock. */
    private final class SingleLoader extends BaseSubscriber<Object> implements Publisher<Value> {
        final Cache cache;
        final Snapshot start;
        final Sinks.One<Value> signal = Sinks.one();
        final Mono<Value> result = Mono.fromDirect(this);
        volatile Object outcome;
        private Context context = Context.empty();
        private boolean started;
        int readers = 1;
        SingleLoader(Cache cache, Snapshot start) { this.cache = cache; this.start = start; }
        @Override public void subscribe(Subscriber<? super Value> actual) {
            // Install the result before starting synchronous work or invoking user code.
            CoreSubscriber<? super Value> reader = Operators.toCoreSubscriber(actual);
            signal.asMono().doFinally(ignored -> release()).subscribe(reader);
            synchronized (cache) {
                if (started || outcome != null) return;
                started = true;
                context = reader.currentContext();
            }
            try { clusterCache.get(cache.key).subscribe(this); }
            catch (Throwable failure) { Exceptions.throwIfFatal(failure); cache.fail(this, failure); }
        }
        @Override public Context currentContext() { return context; }
        @Override protected void hookOnNext(Object value) { cache.complete(this, Value.simple(value)); }
        @Override protected void hookOnComplete() { if (outcome == null) cache.complete(this, NULL); }
        @Override protected void hookOnError(Throwable failure) { cache.fail(this, failure); }
        void release() {
            synchronized (cache) {
                if (--readers != 0 || outcome != null) return;
                if (cache.loading == this) cache.loading = null;
                outcome = new CancellationException("No configuration readers remain");
            }
            // The final reader has already cancelled its sink subscription.
            cancel();
            manager.endLoad(id, this);
        }
        void deliver() {
            Object terminal = outcome;
            if (terminal == null) return;
            cancel();
            if (terminal == NULL) signal.tryEmitEmpty();
            else if (terminal instanceof Value) signal.tryEmitValue((Value) terminal);
            else signal.tryEmitError((Throwable) terminal);
            manager.endLoad(id, this);
        }
        @Override public void dispose() { cache.fail(this, new CancellationException("Storage manager is disposed")); }
    }

    /** An unresolved batch key; no per-key publisher, shared group or reader lease. */
    private static final class BatchEntry {
        final Cache cache;
        final Snapshot start;
        final BatchLoader owner;
        BatchEntry next;
        Value received;
        volatile Object outcome;
        BatchEntry(Cache cache, Snapshot start, BatchLoader owner) {
            this.cache = cache; this.start = start; this.owner = owner;
        }
    }

    /** One batch subscription owns one backend subscription and one bounded result map. */
    private final class BatchLoader extends BaseSubscriber<Map.Entry<String, Object>> implements Publisher<Values> {
        final List<String> keys;
        final Map<String, BatchEntry> targets;
        final Map<String, Object> values;
        final Sinks.One<Values> signal = Sinks.one();
        private Context context = Context.empty();
        private volatile boolean sealed;
        private volatile boolean stopped;
        private volatile Object outcome;
        BatchLoader(List<String> keys) {
            this.keys = keys;
            targets = Maps.newHashMapWithExpectedSize(keys.size());
            values = Maps.newHashMapWithExpectedSize(keys.size());
        }
        @Override public void subscribe(Subscriber<? super Values> actual) {
            CoreSubscriber<? super Values> reader = Operators.toCoreSubscriber(actual);
            signal.asMono().doOnCancel(this::cancelRead).subscribe(reader);
            if (stopped) return;
            context = reader.currentContext();
            if (!manager.startLoad(id, this)) { dispose(); return; }
            try {
                for (String key : keys) {
                    if (stopped) return;
                    Cache cache = getOrCreateCache(key);
                    synchronized (cache) {
                        Snapshot state = cache.cachedState();
                        if (state != null) {
                            if (state.value != NULL) values.put(key, state.value.get());
                        } else {
                            BatchEntry entry = new BatchEntry(cache, cache.current, this);
                            synchronized (this) {
                                if (stopped) return;
                                // The target map already deduplicates keys; no second
                                // input Set or per-key shared group is needed.
                                if (targets.putIfAbsent(key, entry) != null) continue;
                                entry.next = cache.batches;
                                cache.batches = entry;
                            }
                        }
                    }
                }
                sealed = true;
                finishIfReady();
                if (!stopped) clusterCache.get(Collections.unmodifiableSet(targets.keySet())).subscribe(this);
            } catch (Throwable failure) {
                Exceptions.throwIfFatal(failure);
                failed(failure, true);
            }
        }
        @Override public Context currentContext() { return context; }
        @Override protected void hookOnNext(Map.Entry<String, Object> value) {
            BatchEntry entry = targets.get(value.getKey());
            if (entry != null && entry.received == null)
                entry.received = value.getValue() == null ? NULL : Value.simple(value.getValue());
        }
        @Override protected void hookOnComplete() {
            if (stopped) return;
            try {
                // A partial backend failure must never populate the cache or seed retry.
                for (BatchEntry entry : targets.values())
                    entry.cache.complete(entry, entry.received == null ? NULL : entry.received);
                finishIfReady();
            } catch (Throwable failure) {
                Exceptions.throwIfFatal(failure);
                failed(failure, true);
            }
        }
        @Override protected void hookOnError(Throwable failure) { failed(failure, true); }
        void finishIfReady() {
            if (!sealed || stopped) return;
            synchronized (this) {
                if (stopped) return;
                for (BatchEntry entry : targets.values()) if (entry.outcome == null) return;
                outcome = resultValues();
                stopped = true;
            }
            deliver();
        }
        private Values resultValues() {
            for (Map.Entry<String, BatchEntry> target : targets.entrySet()) {
                Object terminal = target.getValue().outcome;
                Object value = ((Value) terminal).get();
                if (value != null) values.put(target.getKey(), value);
            }
            return values.isEmpty() ? EMPTY_VALUES : Values.of(values);
        }
        private void failed(Throwable failure, boolean notify) {
            List<BatchEntry> pending;
            boolean report;
            synchronized (this) {
                report = !stopped && !(failure instanceof CancellationException);
                stopped = true;
                if (failure instanceof CancellationException && outcome == null) outcome = failure;
                pending = new ArrayList<>(targets.values());
            }
            if (report) error(failure);
            for (BatchEntry entry : pending) entry.cache.fail(entry, failure);
            synchronized (this) {
                if (outcome == null) {
                    boolean recovered = sealed && !pending.isEmpty();
                    for (BatchEntry entry : pending) if (!(entry.outcome instanceof Value)) recovered = false;
                    outcome = recovered ? resultValues() : failure;
                }
            }
            if (notify) deliver();
            else { cancel(); manager.endLoad(id, this); }
        }
        private void deliver() {
            cancel();
            Object terminal = outcome;
            if (terminal instanceof Values) signal.tryEmitValue((Values) terminal);
            else signal.tryEmitError((Throwable) terminal);
            manager.endLoad(id, this);
        }
        private void cancelRead() { failed(new CancellationException("Configuration batch reader cancelled"), false); }
        @Override public void dispose() { failed(new CancellationException("Storage manager is disposed"), true); }
    }

    @Override public String toString() { return caches.toString(); }
}
