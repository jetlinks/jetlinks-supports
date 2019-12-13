package org.jetlinks.supports.config;

import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.test.StepVerifier;

public class ClusterConfigStorageManagerTest {

    private ReactiveRedisTemplate<Object, Object> operations = RedisHelper.getRedisTemplate();

    public RedisClusterManager clusterManager;

    private ClusterConfigStorageManager storageManager;


    @Before
    public void init() {
        clusterManager = new RedisClusterManager("default", "test", operations);
        storageManager = new ClusterConfigStorageManager(clusterManager);

    }

    @Test
    public void test() {
        storageManager.getStorage("test")
                .flatMap(configStorage -> configStorage.setConfig("test", "test"))
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

    }

}