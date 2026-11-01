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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

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

    private Cache getOrCreateCache(String key) {
        Cache found = caches.get(key);
        if (found != null) return found;
        // Miss registration and empty-storage removal use the same manager map boundary.
        // A retained handle rejoins the canonical storage after clear/cleanup.
        for (;;) {
            LocalCacheClusterConfigStorage active = manager.cache.compute(id, (name, existing) -> {
                LocalCacheClusterConfigStorage selected = existing == null ? this : existing;
                selected.caches.computeIfAbsent(key, selected::createCache);
                return selected;
            });
            found = active.caches.get(key);
            if (found != null) return found;
        }
    }

    @Override
    public Mono<Value> getConfig(String key) { return getConfig(key, null); }

    @Override
    public Mono<Value> getConfig(String key, Mono<Object> fallback) {
        Cache cache = getOrCreateCache(key);
        Snapshot state = cache.cachedState();
        if (state != null && (state.value != NULL || fallback == null)) return state.result;
        // Resolve again per subscription: retry must claim new work, never replay a failed sink.
        return Mono.defer(() -> singleRead(key, fallback));
    }

    private Mono<Value> singleRead(String key, Mono<Object> fallback) {
        Cache cache = getOrCreateCache(key);
        Snapshot state = cache.cachedState();
        if (state != null && (state.value != NULL || fallback == null)) return state.result;
        ReadPlan plan = new ReadPlan(true, fallback);
        try {
            Mono<Value> result = plan.read(cache, fallback != null);
            plan.seal();
            Mono<Value> read = plan.run(result).filter(value -> value != NULL);
            // A single with a fallback can join a batch/default read. If that shared
            // request finds no value, claim the negative entry and run the fallback.
            if (fallback != null && !plan.leases.isEmpty() && plan.leases.get(0).group.fallback == null) {
                return read.switchIfEmpty(Mono.defer(() -> singleRead(key, fallback)));
            }
            return read;
        } catch (Throwable error) {
            plan.seal();
            plan.release();
            return Mono.error(error);
        }
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
        ReadPlan plan = new ReadPlan(false, null);
        try {
            List<Mono<Value>> results = new ArrayList<>(keys.size());
            for (String key : keys) results.add(plan.read(getOrCreateCache(key), false));
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
            Value first = getOrCreateCache(firstKey).getCached();
            if (first == null) return null;
            String secondKey = it.next();
            Value second = getOrCreateCache(secondKey).getCached();
            if (second == null) return null;
            if (count == 2) return first.get() == null && second.get() == null ? EMPTY_VALUES_MONO
                : Mono.just(Values.of(firstKey, first.get(), secondKey, second.get()));
            String thirdKey = it.next();
            Value third = getOrCreateCache(thirdKey).getCached();
            if (third == null) return null;
            return first.get() == null && second.get() == null && third.get() == null ? EMPTY_VALUES_MONO
                : Mono.just(Values.of(firstKey, first.get(), secondKey, second.get(), thirdKey, third.get()));
        }
        Map<String, Object> values = null;
        for (String key : keys) {
            Value value = getOrCreateCache(key).getCached();
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
        return clusterCache.getAndRemove(key).map(Value::simple)
            .flatMap(value -> notifyRemoveKey(key).thenReturn(value))
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

    public class Cache {
        final String key;
        volatile long t;
        private volatile Snapshot current = EMPTY;
        private volatile Pending loading;
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
            synchronized (this) {
                current = new Snapshot(current.version + 1, value);
                pending = loading;
                loading = null;
                if (pending != null) pending.outcome = value;
                touch();
            }
            if (pending != null) pending.deliver();
        }

        Pending acquire(ReadPlan plan, boolean loadNegative) {
            synchronized (this) {
                Snapshot state = current;
                if (state.value != null && !isExpired() && (!loadNegative || state.value != NULL)) return null;
                if (loading != null && loading.group.retain()) return loading;
                LoadGroup candidate = plan.candidate();
                Pending pending = new Pending(this, state, candidate,
                    state.value == NULL && !isExpired());
                candidate.add(pending);
                loading = pending;
                return pending;
            }
        }

        void complete(Pending pending, Value value) {
            // Sharing can invoke user hashCode. Recheck ownership after it, then publish
            // the value/version and reserve the waiter's outcome in one critical section.
            Value selected = value == null ? NULL : tryShare(key, value);
            synchronized (this) {
                if (pending.outcome == null) {
                    if (loading == pending && current == pending.start) {
                        current = new Snapshot(current.version + 1, selected);
                        touch();
                    } else if (current.value != null) selected = current.value;
                    if (loading == pending) loading = null;
                    pending.outcome = selected;
                }
            }
            pending.deliver();
        }

        void fail(Pending pending, Throwable error) {
            boolean fallback = false;
            synchronized (this) {
                if (pending.outcome == null) {
                    if (loading == pending) loading = null;
                    Value stale = current.value;
                    fallback = stale != null && !(error instanceof CancellationException) && !pending.negativeFallback;
                    pending.outcome = fallback ? stale : error;
                }
            }
            if (fallback) {
                manager.monitor.staleFallbacks.increment();
                log.warn("load cache [{} {}] failed; using a previous value, version:{}", id, key, version(), error);
            }
            pending.deliver();
        }
        @Override public String toString() { return key + ":" + version(); }
    }

    /** Per-key ownership, shared by single and batch plans; each lease belongs to a subscriber. */
    private final class Pending {
        final Cache cache;
        final Snapshot start;
        final LoadGroup group;
        final boolean negativeFallback;
        final Sinks.One<Value> result = Sinks.one();
        volatile Object outcome;
        Pending(Cache cache, Snapshot start, LoadGroup group, boolean negativeHit) {
            this.cache = cache;
            this.start = start;
            this.group = group;
            negativeFallback = negativeHit && group.fallback != null;
        }
        void deliver() {
            Object terminal = outcome;
            if (terminal instanceof Value) result.tryEmitValue((Value) terminal);
            else if (terminal instanceof Throwable) result.tryEmitError((Throwable) terminal);
        }
    }

    /** Registers all result subscribers before driving any possibly synchronous L2 source. */
    private final class ReadPlan {
        final boolean single;
        final Mono<Object> fallback;
        private LoadGroup candidate;
        final List<Pending> leases = new ArrayList<>();
        ReadPlan(boolean single, Mono<Object> fallback) { this.single = single; this.fallback = fallback; }
        LoadGroup candidate() {
            if (candidate == null) candidate = new LoadGroup(single, fallback);
            return candidate;
        }
        void seal() { if (candidate != null) candidate.seal(); }
        Mono<Value> read(Cache cache, boolean fallback) {
            Pending pending = cache.acquire(this, fallback);
            if (pending == null) return Mono.just(cache.current.value);
            leases.add(pending);
            return pending.result.asMono();
        }
        <T> Mono<T> run(Mono<T> result) {
            if (leases.isEmpty()) return result;
            LoadGroup first = leases.get(0).group;
            Set<LoadGroup> groups = null;
            for (Pending pending : leases) {
                if (pending.group != first) {
                    if (groups == null) {
                        groups = new LinkedHashSet<>();
                        groups.add(first);
                    }
                    groups.add(pending.group);
                }
            }
            Mono<Void> execution = groups == null ? first.execution
                : Mono.when(groups.stream().map(group -> group.execution).toArray(Mono[]::new));
            Mono<T> drive = execution.then(Mono.never());
            return Mono.firstWithSignal(result, drive).doFinally(ignored -> release());
        }
        void release() { for (Pending pending : leases) pending.group.release(); }
    }

    /**
     * One remote request for newly claimed keys. Explicit leases keep it alive
     * across different batches; last release cancels it even though execution is cached.
     * Terminal cleanup precedes sink delivery so reentrant retry cannot inherit dead work.
     */
    private final class LoadGroup implements Disposable {
        final boolean single;
        final Mono<Object> fallback;
        final Map<String, Pending> owned = new LinkedHashMap<>();
        final Sinks.One<Void> ready;
        final Sinks.One<Void> stop = Sinks.one();
        final Mono<Void> execution;
        int leases;
        boolean closed;
        volatile boolean resultsReady;
        LoadGroup(boolean single, Mono<Object> fallback) {
            this.single = single;
            this.fallback = fallback;
            // Only a batch can be joined before all of its owned keys have been added.
            ready = single ? null : Sinks.one();
            Mono<Void> start = Mono.defer(this::execute);
            execution = (single ? start : ready.asMono().then(start))
                .onErrorResume(this::failed)
                .doFinally(ignored -> manager.endLoad(this))
                .cache();
        }
        synchronized boolean retain() {
            if (closed) return false;
            leases++;
            return true;
        }
        synchronized void add(Pending pending) {
            if (closed) throw new CancellationException("Configuration load was released");
            owned.put(pending.cache.key, pending);
            leases++;
        }
        void seal() { if (ready != null) ready.tryEmitEmpty(); }
        synchronized List<Pending> pending() { return new ArrayList<>(owned.values()); }
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
                stop.tryEmitError(error);
            }
        }
        Mono<Void> execute() {
            if (!manager.startLoad(this)) return Mono.error(new CancellationException("Storage manager is disposed"));
            List<Pending> keys = pending();
            Mono<Void> source;
            if (single) {
                Pending pending = keys.get(0);
                Mono<Object> read = pending.negativeFallback ? fallback : clusterCache.get(pending.cache.key);
                if (!pending.negativeFallback && fallback != null) read = read.switchIfEmpty(fallback);
                source = read.doOnNext(value -> {
                    resultsReady = true;
                    pending.cache.complete(pending, Value.simple(value));
                }).then(Mono.fromRunnable(() -> {
                    resultsReady = true;
                    pending.cache.complete(pending, NULL);
                }));
            } else {
                Set<String> requested = Collections.unmodifiableSet(new LinkedHashSet<>(owned.keySet()));
                Map<String, Value> loaded = Maps.newHashMapWithExpectedSize(keys.size());
                source = clusterCache.get(requested).doOnNext(entry -> {
                    Pending pending = owned.get(entry.getKey());
                    if (pending != null) loaded.putIfAbsent(entry.getKey(),
                        entry.getValue() == null ? NULL : Value.simple(entry.getValue()));
                }).then(Mono.fromRunnable(() -> {
                    // A failed partial batch must not seed the retry with abandoned results.
                    resultsReady = true;
                    for (Pending pending : keys) pending.cache.complete(pending,
                        loaded.getOrDefault(pending.cache.key, NULL));
                }));
            }
            return Mono.firstWithSignal(stop.asMono(), source);
        }
        Mono<Void> failed(Throwable failure) {
            synchronized (this) { closed = true; }
            if (!(failure instanceof CancellationException)) error(failure);
            for (Pending pending : pending()) pending.cache.fail(pending, failure);
            return Mono.empty();
        }
        @Override public void dispose() {
            CancellationException error = new CancellationException("Storage manager is disposed");
            synchronized (this) { closed = true; }
            for (Pending pending : pending()) pending.cache.fail(pending, error);
            stop.tryEmitError(error);
        }
        @Override public synchronized boolean isDisposed() { return closed; }
    }

    @Override public String toString() { return caches.toString(); }
}
