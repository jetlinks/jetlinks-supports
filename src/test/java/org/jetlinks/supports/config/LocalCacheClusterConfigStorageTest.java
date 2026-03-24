package org.jetlinks.supports.config;

import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LocalCacheClusterConfigStorageTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testGetConfigsConfusion() {
        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());
        when(clusterCache.putAll(anyMap())).thenReturn(Mono.just(true));

        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test",
            manager,
            clusterCache,
            -1,
            null,
            new ConcurrentHashMap<>()
        );

        Map<String, Object> data = new HashMap<>();
        data.put("id", "id-value");
        data.put("name", "name-value");

        // Simulate clusterCache returns entries
        when(clusterCache.get(anyCollection())).thenAnswer(invocation -> {
            return Flux.fromIterable(data.entrySet());
        });

        // First call to load into local cache
        storage.getConfigs(Arrays.asList("id", "name"))
            .as(StepVerifier::create)
            .expectNextMatches(values -> {
                assertEquals("id-value", values.getValue("id").map(Value::asString).orElse(null));
                assertEquals("name-value", values.getValue("name").map(Value::asString).orElse(null));
                return true;
            })
            .verifyComplete();

        // 验证并发更新情况
        Flux.range(0, 1000)
            .parallel()
            .runOn(Schedulers.parallel())
            .flatMap(i -> {
                Map<String, Object> currentData = new HashMap<>();
                String idVal = "id-" + i;
                String nameVal = "name-" + i;
                currentData.put("id", idVal);
                currentData.put("name", nameVal);

                // 模拟另一个线程在后台并发更新
                return storage.setConfigs(currentData)
                    .then(storage.getConfigs(Arrays.asList("id", "name")))
                    .doOnNext(values -> {
                        String id = values.getValue("id").map(Value::asString).orElse("");
                        String name = values.getValue("name").map(Value::asString).orElse("");
                        // 如果获取到的 ID 包含 "name-"，说明发生了混淆
                        if (id.startsWith("name-") || name.startsWith("id-")) {
                           throw new RuntimeException("Confusion detected! id=" + id + ", name=" + name);
                        }
                    });
            })
            .sequential()
            .as(StepVerifier::create)
            .expectNextCount(1000)
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSharedValueConfusion() {
        // Test if tryShare causes confusion
        LocalCacheClusterConfigStorage.addSharedKey("sharedKey1", "sharedKey2");

        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());
        when(clusterCache.put(anyString(), any())).thenReturn(Mono.just(true));
        when(clusterCache.get(anyString())).thenReturn(Mono.empty());

        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test", manager, clusterCache, -1, null
        );

        // Both keys have same value
        storage.setConfig("sharedKey1", "same-value").block();
        storage.setConfig("sharedKey2", "same-value").block();

        // 模拟从 clusterCache 加载回相同的值
        when(clusterCache.get("sharedKey1")).thenReturn(Mono.just("same-value"));
        when(clusterCache.get("sharedKey2")).thenReturn(Mono.just("same-value"));

        Value v1 = storage.getConfig("sharedKey1").block();
        Value v2 = storage.getConfig("sharedKey2").block();

        assertNotNull(v1);
        assertNotNull(v2);
        assertSame(v1, v2); // They should be same instance due to tryShare
        assertEquals("same-value", v1.asString());
        assertEquals("same-value", v2.asString());

        // Now change one
        storage.setConfig("sharedKey1", "new-value").block();
        when(clusterCache.get("sharedKey1")).thenReturn(Mono.just("new-value"));

        v1 = storage.getConfig("sharedKey1").block();
        v2 = storage.getConfig("sharedKey2").block();

        assertEquals("new-value", v1.asString());
        assertEquals("same-value", v2.asString());
        assertNotSame(v1, v2);
    }
}
