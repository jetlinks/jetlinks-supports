package org.jetlinks.supports.config;

import lombok.SneakyThrows;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.Test;

import static org.junit.Assert.*;

public class EventBusStorageManagerTest {


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
}