package org.jetlinks.supports.cluster;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ForwardingConcurrentMap;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.Value;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ClusterSet;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperationBroker;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceProductOperator;
import org.jetlinks.core.device.ProductInfo;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.supports.config.CacheNotify;
import org.jetlinks.supports.config.ConfigStorageCacheNotifier;
import org.jetlinks.supports.config.InMemoryConfigStorageManager;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Set;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.when;

public class ClusterDeviceRegistryTest {

    private ClusterDeviceRegistry registry;
    private ProtocolSupports supports;
    private ClusterManager clusterManager;
    private DeviceOperationBroker broker;

    @Before
    public void init() {
        ProtocolSupport protocol = mock(ProtocolSupport.class);
        supports = mock(ProtocolSupports.class);
        when(supports.getProtocol("test")).thenReturn(Mono.just(protocol));
        when(protocol.onProductRegister(any())).thenReturn(Mono.empty());
        when(protocol.onProductUnRegister(any())).thenReturn(Mono.empty());
        when(protocol.onDeviceRegister(any())).thenReturn(Mono.empty());
        when(protocol.onDeviceUnRegister(any())).thenReturn(Mono.empty());

        clusterManager = mock(ClusterManager.class);
        ClusterSet<String> productBind = mock(ClusterSet.class);
        when(productBind.add(anyString())).thenReturn(Mono.just(true));
        when(clusterManager.<String>getSet(anyString())).thenReturn(productBind);
        broker = mock(DeviceOperationBroker.class);

        registry = createRegistry(new InMemoryConfigStorageManager());
    }

    @Test
    public void shouldCacheProductVersionsIndependently() {
        DeviceProductOperator product = register(null);
        DeviceProductOperator version1 = register("v1");
        DeviceProductOperator version2 = register("v2");

        assertSame(product, registry.getProduct("test").block());
        assertSame(version1, registry.getProduct("test", "v1").block());
        assertSame(version2, registry.getProduct("test", "v2").block());
        assertNotSame(product, version1);
        assertNotSame(version1, version2);
    }

    @Test
    public void shouldOnlyRemoveSpecifiedProductVersion() {
        DeviceProductOperator product = register(null);
        DeviceProductOperator version1 = register("v1");
        DeviceProductOperator version2 = register("v2");

        registry.unregisterProduct("test", "v1").block();

        assertNull(registry.getProduct("test", "v1").block());
        assertSame(product, registry.getProduct("test").block());
        assertSame(version2, registry.getProduct("test", "v2").block());

        DeviceProductOperator registeredAgain = register("v1");
        assertNotSame(version1, registeredAgain);
        assertSame(registeredAgain, registry.getProduct("test", "v1").block());
    }

    @Test
    public void shouldRemoveUnversionedProductIndependently() {
        DeviceProductOperator product = register(null);
        DeviceProductOperator version = register("v1");

        registry.unregisterProduct("test").block();

        assertNull(registry.getProduct("test").block());
        assertSame(version, registry.getProduct("test", "v1").block());
        assertNotSame(product, version);
    }

    @Test
    public void shouldKeepConcurrentVersionUpdates() {
        Flux
            .range(0, 64)
            .flatMap(index -> registry
                .register(createProduct("v" + index))
                .subscribeOn(Schedulers.parallel()), 64)
            .blockLast();

        for (int index = 0; index < 64; index++) {
            assertNotNull(registry.getProduct("test", "v" + index).block());
        }

        Flux
            .range(0, 32)
            .flatMap(index -> registry
                .unregisterProduct("test", "v" + (index * 2 + 1))
                .subscribeOn(Schedulers.parallel()), 32)
            .blockLast();

        for (int index = 0; index < 64; index++) {
            DeviceProductOperator operator = registry.getProduct("test", "v" + index).block();
            if ((index & 1) == 0) {
                assertNotNull(operator);
            } else {
                assertNull(operator);
            }
        }
    }

    @Test
    public void shouldInvalidateValidatedDeviceWhenProductBindingChanges() {
        NotifyingConfigStorageManager manager = new NotifyingConfigStorageManager();
        ClusterDeviceRegistry registry = createRegistry(manager);
        registry.register(createProduct(null)).block();
        DeviceOperator device = registry.register(createDevice()).block();

        assertSame(device, registry.getDevice("device").block());
        assertSame(device, registry.getDevice("device").block());

        ConfigStorage storage = manager.getStorage("device:device").block();
        storage.remove(DeviceConfigKey.productId.getKey()).block();
        manager.emit(CacheNotify.expires(
            "device:device",
            Set.of(DeviceConfigKey.productId.getKey())
        ));

        assertNull(registry.getDevice("device").block());
    }

    @Test
    public void shouldInvalidateValidatedDevicesWhenProductIsRemoved() {
        NotifyingConfigStorageManager manager = new NotifyingConfigStorageManager();
        ClusterDeviceRegistry registry = createRegistry(manager);
        registry.register(createProduct(null)).block();
        registry.register(createDevice()).block();

        assertNotNull(registry.getDevice("device").block());

        manager.getStorage("device-product:test").block().clear().block();
        manager.emit(CacheNotify.clear("device-product:test"));

        assertNull(registry.getDevice("device").block());
    }

    @Test
    public void shouldNotCacheProductLoadedBeforeInvalidation() {
        NotifyingConfigStorageManager manager = new NotifyingConfigStorageManager();
        ClusterDeviceRegistry registry = createRegistry(manager);
        registry.register(createProduct("v1")).block();
        manager.pauseNextProtocolLookup("device-product:test:v1");

        StepVerifier.create(registry.getProduct("test", "v1"))
                    .then(() -> {
                        manager.getStorage("device-product:test:v1").block().clear().block();
                        manager.emit(CacheNotify.clear("device-product:test:v1"));
                        manager.releasePausedLookup();
                    })
                    .expectNextCount(1)
                    .verifyComplete();

        assertNull(registry.getProduct("test", "v1").block());
    }

    @Test
    public void shouldNotReinsertUnversionedProductWhenInvalidatedDuringCacheWrite() throws Exception {
        assertInvalidationWinsOverInFlightWrite(null, "productOperatorMap");
    }

    @Test
    public void shouldNotReinsertVersionedProductWhenInvalidatedDuringCacheWrite() throws Exception {
        assertInvalidationWinsOverInFlightWrite("v1", "versionedProductOperatorMap");
    }

    private void assertInvalidationWinsOverInFlightWrite(String version, String fieldName) throws Exception {
        NotifyingConfigStorageManager manager = new NotifyingConfigStorageManager();
        ClusterDeviceRegistry registry = createRegistry(manager);
        registry.register(createProduct(version)).block();

        PausedWriteMap<String, Object> map = new PausedWriteMap<>();
        Field cacheField = ClusterDeviceRegistry.class.getDeclaredField(fieldName);
        cacheField.setAccessible(true);
        cacheField.set(registry, map);

        CompletableFuture<DeviceProductOperator> lookup = CompletableFuture.supplyAsync(
            () -> registry.getProduct("test", version).block()
        );
        assertTrue(map.entered.await(5, TimeUnit.SECONDS));

        CompletableFuture<Void> invalidation = CompletableFuture.runAsync(
            () -> manager.emit(CacheNotify.clear("device-product:test"))
        );
        try {
            try {
                invalidation.get(200, TimeUnit.MILLISECONDS);
            } catch (TimeoutException expected) {
                // 写入与失效互斥时，失效将等待写入完成。
            }
        } finally {
            map.release.countDown();
        }
        assertNotNull(lookup.get(5, TimeUnit.SECONDS));
        invalidation.get(5, TimeUnit.SECONDS);
        assertNull(map.get("test"));
    }

    private static final class PausedWriteMap<K, V> extends ForwardingConcurrentMap<K, V> {
        private final ConcurrentMap<K, V> entries = new ConcurrentHashMap<>();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        protected ConcurrentMap<K, V> delegate() {
            return entries;
        }

        @Override
        public V put(K key, V value) {
            pauseWrite();
            return entries.put(key, value);
        }

        @Override
        public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            pauseWrite();
            return entries.compute(key, remappingFunction);
        }

        private void pauseWrite() {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out awaiting cache write");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(error);
            }
        }
    }

    @Test
    public void shouldKeepPerSubscriptionValidationWithoutNotifier() {
        InMemoryConfigStorageManager manager = new InMemoryConfigStorageManager();
        ClusterDeviceRegistry registry = createRegistry(manager);
        registry.register(createProduct(null)).block();
        registry.register(createDevice()).block();

        assertNotNull(registry.getDevice("device").block());
        manager
            .getStorage("device:device")
            .flatMap(storage -> storage.remove(DeviceConfigKey.productId.getKey()))
            .block();

        assertNull(registry.getDevice("device").block());
    }

    @Test
    public void shouldDisposeCacheNotifyListener() {
        NotifyingConfigStorageManager manager = new NotifyingConfigStorageManager();
        ClusterDeviceRegistry registry = createRegistry(manager);

        assertEquals(1, manager.listenerCount());

        registry.dispose();

        assertEquals(0, manager.listenerCount());
    }

    @Test
    public void shouldDisposeAddedInterceptors() {
        ClusterDeviceRegistry registry = createRegistry(new InMemoryConfigStorageManager());
        DeviceMessageSenderInterceptor interceptor = mock(
            DeviceMessageSenderInterceptor.class,
            withSettings().extraInterfaces(Disposable.class)
        );
        AtomicReference<Boolean> disposed = new AtomicReference<>(false);
        doAnswer(ignore -> {
            disposed.set(true);
            return null;
        }).when((Disposable) interceptor).dispose();

        registry.addInterceptor(interceptor);
        registry.dispose();

        assertTrue(disposed.get());
    }

    @Test
    public void shouldContinueRegistryDisposalAfterInterceptorFailure() {
        NotifyingConfigStorageManager manager = new NotifyingConfigStorageManager();
        ClusterDeviceRegistry registry = createRegistry(manager);
        DeviceMessageSenderInterceptor failing = mock(
            DeviceMessageSenderInterceptor.class,
            withSettings().extraInterfaces(Disposable.class)
        );
        DeviceMessageSenderInterceptor following = mock(
            DeviceMessageSenderInterceptor.class,
            withSettings().extraInterfaces(Disposable.class)
        );
        AtomicReference<Boolean> disposed = new AtomicReference<>(false);
        doAnswer(ignore -> {
            throw new IllegalStateException("boom");
        }).when((Disposable) failing).dispose();
        doAnswer(ignore -> {
            disposed.set(true);
            return null;
        }).when((Disposable) following).dispose();
        registry.addInterceptor(failing);
        registry.addInterceptor(following);

        try {
            registry.dispose();
            fail("expected dispose failure");
        } catch (RuntimeException error) {
            assertTrue(error.getCause() instanceof IllegalStateException);
        }

        assertEquals(0, manager.listenerCount());
        assertTrue(disposed.get());
    }

    private DeviceProductOperator register(String version) {
        return registry.register(createProduct(version)).block();
    }

    private ProductInfo createProduct(String version) {
        return ProductInfo
            .builder()
            .id("test")
            .version(version)
            .protocol("test")
            .build();
    }

    private DeviceInfo createDevice() {
        return DeviceInfo
            .builder()
            .id("device")
            .productId("test")
            .build();
    }

    private ClusterDeviceRegistry createRegistry(ConfigStorageManager storageManager) {
        return new ClusterDeviceRegistry(
            supports,
            storageManager,
            clusterManager,
            broker,
            CacheBuilder.newBuilder().build()
        );
    }

    private static final class NotifyingConfigStorageManager
        implements ConfigStorageManager, ConfigStorageCacheNotifier {

        private final InMemoryConfigStorageManager delegate = new InMemoryConfigStorageManager();
        private final Set<Consumer<CacheNotify>> listeners = new CopyOnWriteArraySet<>();
        private final AtomicReference<PausedLookup> pausedLookup = new AtomicReference<>();

        @Override
        public Mono<ConfigStorage> getStorage(String id) {
            return delegate.getStorage(id)
                           .map(storage -> new ConfigStorage() {
                               @Override
                               public Mono<Value> getConfig(String key) {
                                   PausedLookup paused = pausedLookup.get();
                                   if (paused != null
                                       && paused.matches(id, key)) {
                                       Value snapshot = storage.getConfig(key).block();
                                       return paused.await.asMono().then(Mono.justOrEmpty(snapshot));
                                   }
                                   return storage.getConfig(key);
                               }

                               @Override
                               public Mono<org.jetlinks.core.Values> getConfigs(java.util.Collection<String> key) {
                                   return storage.getConfigs(key);
                               }

                               @Override
                               public Mono<Boolean> setConfigs(java.util.Map<String, Object> values) {
                                   return storage.setConfigs(values);
                               }

                               @Override
                               public Mono<Boolean> setConfig(String key, Object value) {
                                   return storage.setConfig(key, value);
                               }

                               @Override
                               public Mono<Boolean> remove(String key) {
                                   return storage.remove(key);
                               }

                               @Override
                               public Mono<Value> getAndRemove(String key) {
                                   return storage.getAndRemove(key);
                               }

                               @Override
                               public Mono<Boolean> remove(java.util.Collection<String> key) {
                                   return storage.remove(key);
                               }

                               @Override
                               public Mono<Boolean> clear() {
                                   return storage.clear();
                               }
                           });
        }

        @Override
        public Disposable listenCacheNotify(Consumer<CacheNotify> listener) {
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }

        private void pauseNextProtocolLookup(String storageId) {
            pausedLookup.set(new PausedLookup(storageId, Sinks.empty()));
        }

        private void releasePausedLookup() {
            PausedLookup paused = pausedLookup.getAndSet(null);
            if (paused != null) {
                paused.await.tryEmitEmpty();
            }
        }

        private int listenerCount() {
            return listeners.size();
        }

        private void emit(CacheNotify notify) {
            listeners.forEach(listener -> listener.accept(notify));
        }

        private static final class PausedLookup {
            private final String storageId;
            private final Sinks.Empty<Void> await;

            private PausedLookup(String storageId, Sinks.Empty<Void> await) {
                this.storageId = storageId;
                this.await = await;
            }

            private boolean matches(String storageId, String key) {
                return this.storageId.equals(storageId)
                    && DeviceConfigKey.protocol.getKey().equals(key);
            }
        }
    }
}
