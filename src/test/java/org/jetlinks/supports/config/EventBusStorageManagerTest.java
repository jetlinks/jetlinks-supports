package org.jetlinks.supports.config;

import lombok.SneakyThrows;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventBusStorageManagerTest {

    @Test
    public void shouldNotifyLocalCacheListeners() {
        EventBusStorageManager manager = new EventBusStorageManager(
            org.mockito.Mockito.mock(org.jetlinks.core.cluster.ClusterManager.class),
            new InternalEventBus()
        );
        AtomicReference<CacheNotify> received = new AtomicReference<>();
        Disposable listener = manager.listenCacheNotify(received::set);
        CacheNotify notify = CacheNotify.clear("device:test");

        manager.doNotify(notify).block();

        assertSame(notify, received.get());
        listener.dispose();
        manager.dispose();
    }

    @Test
    public void shouldNotifyLocalCacheListenersWhenRefreshingAll() {
        ClusterManager clusterManager = mock(ClusterManager.class);
        ClusterCache<String, Object> cache = mock(ClusterCache.class);
        when(clusterManager.<String, Object>createCache("device:test")).thenReturn(cache);
        EventBusStorageManager manager = new EventBusStorageManager(
            clusterManager,
            new InternalEventBus()
        );
        manager.getStorage("device:test").block();
        AtomicReference<CacheNotify> received = new AtomicReference<>();
        manager.listenCacheNotify(received::set);

        manager.refreshAll();

        assertNotNull(received.get());
        assertEquals("device:test", received.get().getName());
        manager.dispose();
    }


    @Test
    @SneakyThrows
    public void testTTL() {

        RedisClusterManager clusterManager = new RedisClusterManager(
            "ttl-test",
            "ttl-test",
            RedisHelper.getRedisTemplate()
        );
        EventBusStorageManager manager = new EventBusStorageManager(
            clusterManager,
            new InternalEventBus(),
            1000
        );
        ConfigStorage storage = manager.getStorage("test_ttl").block();
        assertNotNull(storage);
        assertEquals(1, manager.cache.size());

        storage.setConfig("test", "test").block();

        Thread.sleep(1001);
        manager.cleanup();

        assertEquals(0, manager.cache.size());

    }

    @Test
    @SneakyThrows
    public void testPar() {
        RedisClusterManager clusterManager = new RedisClusterManager(
            "testPar",
            "testPar",
            RedisHelper.getRedisTemplate()
        );
        EventBusStorageManager manager = new EventBusStorageManager(
            clusterManager,
            new InternalEventBus(),
            -1
        );
        ConfigStorage storage = manager.getStorage("test_par").block();
        assertNotNull(storage);

        storage.setConfig("productId", "test").block();

        Flux.range(0, 10000)
            .flatMap(i -> Mono
                .zip(
                    i % 10 == 0 ? Mono.just(i) : storage
                        .refresh()
                        .subscribeOn(Schedulers.boundedElastic())
                        .thenReturn(1),
                    storage.getConfig("productId")
                           .subscribeOn(Schedulers.parallel()),
                    storage.getConfig("productId")
                           .subscribeOn(Schedulers.boundedElastic()),
                    Mono.fromRunnable(() -> storage.getConfig("productId").subscribe().dispose())
                        .thenReturn(1)
                        .subscribeOn(Schedulers.parallel()),
                    Mono.fromRunnable(() -> storage.getConfig("productId").subscribe().dispose())
                        .thenReturn(1)
                        .subscribeOn(Schedulers.boundedElastic()),
                    storage.getConfig("productId")
                           .subscribeOn(Schedulers.parallel()),
                    storage.getConfigs("productId", "deviceId")
                           .subscribeOn(Schedulers.boundedElastic()),
                    storage.getConfigs("productId", "deviceId")
                           .subscribeOn(Schedulers.parallel())
                )
                .subscribeOn(Schedulers.boundedElastic()),
                     Integer.MAX_VALUE)
            .as(StepVerifier::create)
            .expectNextCount(10000)
            .verifyComplete();

        System.out.println(storage);
    }
}
