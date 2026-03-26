package org.jetlinks.supports.config;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Slf4j
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
    @Test
    @SuppressWarnings("unchecked")
    public void testAwaitLeakOnCompleteMismatch() {
        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());

        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test",
            manager,
            clusterCache,
            -1,
            null,
            new ConcurrentHashMap<>()
        );

        // 模拟一个加载缓慢的空配置
        when(clusterCache.get(anyString())).thenReturn(Mono.empty().delaySubscription(Duration.ofMillis(100)));

        Mono<Value> result = storage.getConfig("test_key");

        // 1. 触发第一次获取，进入 reload 流程
        result.subscribe();

        when(clusterCache.put(anyString(), any())).thenReturn(Mono.just(true));

        // 等待 Loader(0) 启动
        try { Thread.sleep(50); } catch (InterruptedException ignore) {}

        // 2. 触发 version 变更。这会增加版本号并清空 version=0 对应的 await。
        // 但 Loader(0) 内部仍然持有 version=0，并在 100ms 后完成。
        storage.setConfig("test_key", "value").block();

        // 3. 验证之前的 result (Loader(0)) 是否能正确完成。
        // 在修复前，它会因为 version mismatch 而既不完成也不报错，导致 StepVerifier 超时。
        StepVerifier.create(result)
            .expectSubscription()
            .expectComplete()
            .verify(Duration.ofMillis(500));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAwaitLeakOnError() {
        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());

        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test",
            manager,
            clusterCache,
            -1,
            null,
            new ConcurrentHashMap<>()
        );

        // 模拟加载出错
        when(clusterCache.get(anyString())).thenReturn(Mono.error(new RuntimeException("test error")));

        Mono<Value> result = storage.getConfig("test_key");

        StepVerifier.create(result)
            .expectError(RuntimeException.class)
            .verify(Duration.ofMillis(500));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrentGetSameKey() {
        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        when(clusterCache.get(anyString())).thenReturn(Mono.empty());
        when(clusterCache.get(anyCollection())).thenAnswer(inv -> Flux.empty());
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());

        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test", manager, clusterCache, -1, null
        );

        String key = "test_key";
        String value = "test_value";

        // 模拟慢加载
        when(clusterCache.get(key)).thenReturn((Mono) Mono.just(value).delayElement(Duration.ofMillis(200)));

        // 并发 100 个请求同一个 key
        Flux.range(0, 100)
            .flatMap(i -> storage.getConfig(key).subscribeOn(Schedulers.parallel()))
            .as(StepVerifier::create)
            .expectNextCount(100)
            .verifyComplete();

        // 验证 clusterCache.get 只被调用了一次
        verify(clusterCache, times(1)).get(key);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrentClearWhileLoading() {
        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        when(clusterCache.get(anyString())).thenReturn(Mono.empty());
        when(clusterCache.get(anyCollection())).thenAnswer(inv -> Flux.empty());
        when(clusterCache.clear()).thenReturn((Mono) Mono.just(true));
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());

        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test", manager, clusterCache, -1, null
        );

        String key = "test_key";
        // 模拟一个永远不完成的加载，或者极慢的加载
        Sinks.One<Object> sink = Sinks.one();
        when(clusterCache.get(key)).thenReturn((Mono) sink.asMono());

        // 启动加载
        Mono<Value> result = storage.getConfig(key);
        result.subscribe();

        // 此时正在 loading
        storage.clear().block();

        // 即使 clear 了，之前的 await 应该也能通过某种方式结束，或者至少不导致内存泄漏
        // 在目前的实现中，clear 会清理 caches map。
        // 如果 clear 后加载完成了，会发生什么？
        sink.emitValue("value", Sinks.EmitFailureHandler.FAIL_FAST);

        // 验证再次获取能拿到新值（如果 clusterCache 有值的话）
        when(clusterCache.get(key)).thenReturn(Mono.just("new_value"));
        storage.getConfig(key)
            .map(Value::asString)
            .as(StepVerifier::create)
            .expectNext("new_value")
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExpirationStress() throws InterruptedException {
        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        when(clusterCache.get(anyString())).thenReturn(Mono.empty());
        when(clusterCache.get(anyCollection())).thenAnswer(inv -> Flux.empty());
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());

        // 设置极短的过期时间
        long expires = 10;
        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test", manager, clusterCache, expires, null
        );

        when(clusterCache.get(anyString())).thenAnswer(inv -> Mono.just("value-" + inv.getArgument(0)));

        AtomicInteger errorCount = new AtomicInteger();
        // 压力测试：并发获取和过期清理
        Flux.range(0, 1000)
            .parallel()
            .runOn(Schedulers.parallel())
            .flatMap(i -> {
                String key = "key-" + (i % 10);
                return storage.getConfig(key)
                    .doOnNext(v -> {
                        if (!v.asString().equals("value-" + key)) {
                            errorCount.incrementAndGet();
                        }
                    })
                    .then(Mono.fromRunnable(storage::cleanup));
            })
            .sequential()
            .collectList()
            .block();

        assertEquals(0, errorCount.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMixedConcurrentOps() {
        ClusterCache<String, Object> clusterCache = mock(ClusterCache.class);
        when(clusterCache.get(anyString())).thenReturn(Mono.empty());
        // 使用 answer 确保返回类型一致
        when(clusterCache.get(anyCollection())).thenAnswer(inv -> Flux.empty());
        when(clusterCache.clear()).thenReturn((Mono) Mono.just(true));
        EventBusStorageManager manager = mock(EventBusStorageManager.class);
        when(manager.doNotify(any())).thenReturn(Mono.empty());
        when(clusterCache.put(anyString(), any())).thenReturn((Mono) Mono.just(true));
        when(clusterCache.putAll(anyMap())).thenReturn((Mono) Mono.just(true));
        when(clusterCache.remove(anyString())).thenReturn((Mono) Mono.just(true));
        when(clusterCache.remove(anyCollection())).thenReturn((Mono) Mono.just(true));

        LocalCacheClusterConfigStorage storage = new LocalCacheClusterConfigStorage(
            "test", manager, clusterCache, -1, null
        );

        AtomicInteger errorCount = new AtomicInteger();
        // 模拟各种混合并发操作
        IntStream.range(0, 5000)
            .parallel()
            .forEach(i -> {
                String key = "key-" + (i % 50);
                double rand = Math.random();
                Mono<?> op;
                if (rand < 0.4) {
                    // 这里不能用 when(clusterCache.get(key)).thenReturn(...) 因为并发下 Mockito 会乱
                    // 已经在上面通用设置了
                    op = storage.getConfig(key);
                } else if (rand < 0.7) {
                    op = storage.setConfig(key, "v-" + i);
                } else if (rand < 0.9) {
                    op = storage.remove(key);
                } else {
                    op = storage.getConfigs(Arrays.asList("key-1", "key-2", "key-3"));
                }
                op.doOnError(err -> {
                    log.error("Error in concurrent op: {}", err.getMessage(), err);
                    errorCount.incrementAndGet();
                }).subscribe();
            });

        assertEquals(0, errorCount.get());
    }
}
