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
import reactor.core.Disposable;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
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
 * single and batch subscribers share pending work without sharing mutable result maps.
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
        if (expires > 0) caches.forEach((key, cache) -> {
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
        ReadPlan<Value> plan = new ReadPlan<>(true);
        try {
            Mono<Value> result = plan.read(cache);
            plan.seal();
            return plan.run(result).filter(value -> value != NULL);
        } catch (Throwable error) {
            Exceptions.throwIfFatal(error);
            plan.seal();
            plan.release();
            return Mono.error(error);
        }
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
        List<String> requested = new ArrayList<>(new LinkedHashSet<>(keys));
        return Mono.defer(() -> batchRead(requested));
    }

    private Mono<Values> batchRead(List<String> keys) {
        ReadPlan<Values> plan = new ReadPlan<>(false);
        try {
            List<Mono<Value>> results = new ArrayList<>(keys.size());
            for (String key : keys) results.add(plan.read(getOrCreateCache(key)));
            plan.seal();
            Mono<Values> result = Mono.zip(results, values -> {
                Map<String, Object> container = Maps.newHashMapWithExpectedSize(keys.size());
                for (int i = 0; i < values.length; i++) {
                    Object value = ((Value) values[i]).get();
                    if (value != null) container.put(keys.get(i), value);
                }
                return container.isEmpty() ? EMPTY_VALUES : Values.of(container);
            });
            return plan.run(result);
        } catch (Throwable error) {
            Exceptions.throwIfFatal(error);
            plan.seal();
            plan.release();
            return Mono.error(error);
        }
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

    public class Cache {
        final String key;
        volatile long t;
        private volatile Snapshot current = EMPTY;
        private volatile Pending loading;
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
            Pending pending;
            Set<FallbackLoad> defaults;
            synchronized (this) {
                current = new Snapshot(current.version + 1, value);
                pending = loading;
                loading = null;
                if (pending != null) pending.outcome = value;
                defaults = resolveFallbacks(value);
                touch();
            }
            if (pending != null) pending.deliver();
            deliverFallbacks(defaults, null);
        }

        Pending acquire(ReadPlan<?> plan) {
            synchronized (this) {
                Snapshot state = current;
                if (state.value != null && !isExpired()) return null;
                if (loading != null && plan.retain(loading.group)) return loading;
                LoadGroup candidate = plan.candidate();
                Pending pending = new Pending(this, state, candidate);
                candidate.add(pending);
                loading = pending;
                return pending;
            }
        }

        void complete(Pending pending, Value value) {
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

        void fail(Pending pending, Throwable error) {
            boolean fallback = false;
            synchronized (this) {
                if (pending.outcome == null) {
                    if (loading == pending) loading = null;
                    Value stale = current.value;
                    fallback = stale != null && !(error instanceof CancellationException);
                    pending.outcome = fallback ? stale : error;
                }
            }
            if (fallback) {
                manager.monitor.staleFallbacks.increment();
                log.warn("load cache [{} {}] failed; using a previous value, version:{}", id, key, version(), error);
            }
            pending.deliver();
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
            Pending pending = null;
            Set<FallbackLoad> defaults = null;
            synchronized (this) {
                if (load.outcome == null) {
                    if (selected != NULL && current == load.start) {
                        current = new Snapshot(current.version + 1, selected);
                        touch();
                        pending = loading;
                        loading = null;
                        if (pending != null) pending.outcome = selected;
                        defaults = resolveFallbacks(selected);
                    } else {
                        removeFallback(load);
                        load.outcome = current.value != null && current.value != NULL ? current.value : selected;
                    }
                }
            }
            if (pending != null) pending.deliver();
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

    /** Per-key ownership, shared by single and batch plans; each lease belongs to a subscriber. */
    private final class Pending {
        final Cache cache;
        final Snapshot start;
        final LoadGroup group;
        final Sinks.One<Value> result = Sinks.one();
        volatile Object outcome;
        Pending(Cache cache, Snapshot start, LoadGroup group) {
            this.cache = cache;
            this.start = start;
            this.group = group;
        }
        void deliver() {
            Object terminal = outcome;
            if (terminal instanceof Value) result.tryEmitValue((Value) terminal);
            else if (terminal instanceof Throwable) result.tryEmitError((Throwable) terminal);
        }
    }

    /** Registers all result subscribers before driving any possibly synchronous L2 source. */
    private final class ReadPlan<T> extends Mono<T> {
        final boolean single;
        private LoadGroup candidate;
        private LoadGroup first;
        private Set<LoadGroup> others;
        private Mono<T> result;
        ReadPlan(boolean single) { this.single = single; }
        LoadGroup candidate() {
            if (candidate == null) {
                candidate = new LoadGroup(single);
                remember(candidate);
            }
            return candidate;
        }
        private void remember(LoadGroup group) {
            if (first == null) first = group;
            else {
                if (others == null) others = new LinkedHashSet<>();
                others.add(group);
            }
        }
        boolean retain(LoadGroup group) {
            if (group == first || others != null && others.contains(group)) return !group.isDisposed();
            if (!group.retain()) return false;
            remember(group);
            return true;
        }
        void seal() { if (candidate != null) candidate.seal(); }
        Mono<Value> read(Cache cache) {
            Pending pending = cache.acquire(this);
            if (pending == null) return Mono.just(cache.current.value);
            return pending.result.asMono();
        }
        Mono<T> run(Mono<T> result) {
            if (first == null) return result;
            this.result = result;
            return this;
        }
        @Override public void subscribe(CoreSubscriber<? super T> actual) {
            // Install result subscribers before a synchronous L2 can complete. The
            // result owns demand and the lease; source completion is not a second result.
            result.doFinally(ignored -> release()).subscribe(actual);
            Context context = actual.currentContext();
            first.start(context);
            if (others != null) for (LoadGroup group : others) group.start(context);
        }
        void release() {
            if (first != null) first.release();
            if (others != null) for (LoadGroup group : others) group.release();
        }
    }

    /**
     * One remote request for newly claimed keys. Explicit leases keep it alive
     * across different batches; last release cancels the actual upstream subscription.
     * Terminal cleanup precedes sink delivery so reentrant retry cannot inherit dead work.
     */
    private final class LoadGroup extends BaseSubscriber<Object> {
        final boolean single;
        final Map<String, Pending> owned;
        private Pending only;
        private volatile Context context;
        private boolean sealed;
        private boolean started;
        int leases = 1;
        boolean closed;
        volatile boolean resultsReady;
        LoadGroup(boolean single) {
            this.single = single;
            owned = single ? null : new LinkedHashMap<>();
            sealed = single;
            // Register before publishing Pending: close also covers batch collection.
            if (!manager.startLoad(id, this)) {
                dispose();
                throw new CancellationException("Storage manager is disposed");
            }
        }
        synchronized boolean retain() {
            if (closed) return false;
            leases++;
            return true;
        }
        synchronized void add(Pending pending) {
            if (closed) throw new CancellationException("Configuration load was released");
            if (single) only = pending;
            else owned.put(pending.cache.key, pending);
        }
        void seal() {
            Context selected;
            synchronized (this) { sealed = true; selected = context; }
            if (selected != null) start(selected);
        }
        // Iterated only after seal or close; add() cannot mutate this collection then.
        synchronized Collection<Pending> pending() {
            return single ? only == null ? Collections.emptyList() : Collections.singletonList(only) : owned.values();
        }
        void start(Context selected) {
            synchronized (this) {
                if (context == null) context = selected;
                if (started || !sealed || closed) return;
                started = true;
            }
            try {
                execute().subscribe(this);
            } catch (Throwable failure) {
                Exceptions.throwIfFatal(failure);
                onError(failure);
            }
        }
        @Override public Context currentContext() { return context == null ? Context.empty() : context; }
        @Override protected void hookOnNext(Object value) {
            resultsReady = true;
            only.cache.complete(only, Value.simple(value));
        }
        @Override protected void hookOnComplete() {
            if (single && !resultsReady) {
                resultsReady = true;
                only.cache.complete(only, NULL);
            }
        }
        @Override protected void hookOnError(Throwable failure) { failed(failure); }
        @Override protected void hookFinally(SignalType type) { manager.endLoad(id, this); }
        void release() {
            boolean cancel;
            synchronized (this) {
                boolean last = --leases == 0 && !closed;
                cancel = last && !resultsReady;
                if (last) closed = true;
            }
            if (cancel) {
                CancellationException error = new CancellationException("No configuration readers remain");
                for (Pending pending : pending()) pending.cache.fail(pending, error);
                cancel();
                manager.endLoad(id, this);
            }
        }
        Mono<?> execute() {
            // The subscriber consumes a single value directly; batches emit only after
            // every record was collected, so a partial error cannot publish stale keys.
            if (single) return clusterCache.get(only.cache.key);
            else {
                Set<String> requested = Collections.unmodifiableSet(owned.keySet());
                Map<String, Value> loaded = Maps.newHashMapWithExpectedSize(owned.size());
                return clusterCache.get(requested).doOnNext(entry -> {
                    Pending pending = owned.get(entry.getKey());
                    if (pending != null) loaded.putIfAbsent(entry.getKey(),
                        entry.getValue() == null ? NULL : Value.simple(entry.getValue()));
                }).then(Mono.fromRunnable(() -> {
                    // A failed partial batch must not seed the retry with abandoned results.
                    resultsReady = true;
                    for (Pending pending : owned.values()) pending.cache.complete(pending,
                        loaded.getOrDefault(pending.cache.key, NULL));
                }));
            }
        }
        void failed(Throwable failure) {
            synchronized (this) { closed = true; }
            if (!(failure instanceof CancellationException)) error(failure);
            for (Pending pending : pending()) pending.cache.fail(pending, failure);
        }
        @Override public void dispose() {
            CancellationException error = new CancellationException("Storage manager is disposed");
            synchronized (this) { closed = true; }
            for (Pending pending : pending()) pending.cache.fail(pending, error);
            cancel();
            manager.endLoad(id, this);
        }
        @Override public synchronized boolean isDisposed() { return closed; }
    }

    @Override public String toString() { return caches.toString(); }
}
