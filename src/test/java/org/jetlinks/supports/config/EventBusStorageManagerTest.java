package org.jetlinks.supports.config;

import lombok.SneakyThrows;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.core.Value;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.supports.cluster.RedisHelper;
import org.jetlinks.supports.cluster.event.RedisClusterEventBroker;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.event.BrokerEventBus;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.test.StepVerifier;

public class EventBusStorageManagerTest {

    private final ReactiveRedisTemplate<Object, Object> operations = RedisHelper.getRedisTemplate();

    @Test
    @SneakyThrows
    public void test() {

        EventBusStorageManager storageManager, storageManager2;

        {
            RedisClusterManager clusterManager = new RedisClusterManager("default2", "test", operations);
            clusterManager.startup();
            BrokerEventBus eventBus = new BrokerEventBus();
            eventBus.addBroker(new RedisClusterEventBroker(clusterManager, operations.getConnectionFactory()));
            storageManager = new EventBusStorageManager(clusterManager, eventBus);
        }
        {
            RedisClusterManager clusterManager = new RedisClusterManager("default2", "test2", operations);
            clusterManager.startup();
            BrokerEventBus eventBus = new BrokerEventBus();
            eventBus.addBroker(new RedisClusterEventBroker(clusterManager, operations.getConnectionFactory()));
            storageManager2 = new EventBusStorageManager(clusterManager, eventBus);
        }
        try {
            String id = IDGenerator.UUID.generate();
            storageManager
                    .getStorage(id)
                    .flatMap(storage -> storage.setConfig("test", 1234))
                    .then(storageManager2.getStorage(id))
                    .flatMap(storage -> storage.getConfig("test").map(Value::asInt))
                    .as(StepVerifier::create)
                    .expectNext(1234)
                    .verifyComplete()
            ;

            storageManager
                    .getStorage(id)
                    .flatMap(storage -> storage.setConfig("test", 12345))
                    .then(storageManager2.getStorage(id))
                    .flatMap(storage -> storage.getConfig("test").map(Value::asInt))
                    .as(StepVerifier::create)
                    .expectNext(12345)
                    .verifyComplete()
            ;

            storageManager
                    .getStorage(id)
                    .flatMap(storage -> storage.setConfig("test", null))
                    .then(storageManager2.getStorage(id))
                    .flatMap(storage -> storage.getConfig("test").map(Value::asInt).defaultIfEmpty(1))
                    .as(StepVerifier::create)
                    .expectNext(1)
                    .verifyComplete()
            ;

            storageManager
                    .getStorage(id)
                    .flatMap(ConfigStorage::clear)
                    .then()
                    .as(StepVerifier::create)
                    .expectComplete()
                    .verify()
            ;
            Thread.sleep(100);
            Assert.assertNull(storageManager2.cache.get(id));

        }finally {

        }
    }
}