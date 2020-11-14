package org.jetlinks.supports.cluster;

import lombok.SneakyThrows;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.RepayableDeviceMessage;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.supports.cluster.event.RedisClusterEventBroker;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.event.BrokerEventBus;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;

public class EventBusDeviceOperationBrokerTest {


    EventBusDeviceOperationBroker broker1, broker2;

    @Before
    @SneakyThrows
    public void init() {
        {
           ReactiveRedisTemplate<Object, Object> operations = RedisHelper.getRedisTemplate();

            BrokerEventBus eventBus = new BrokerEventBus();
            RedisClusterManager clusterManager = new RedisClusterManager("test123", "node-1", operations);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus.addBroker(new RedisClusterEventBroker(clusterManager, operations.getConnectionFactory()));

            broker1 = new EventBusDeviceOperationBroker("node-1", eventBus);
            broker1.start();
        }

        {
            ReactiveRedisTemplate<Object, Object> operations = RedisHelper.getRedisTemplate();

            BrokerEventBus eventBus = new BrokerEventBus();
            RedisClusterManager clusterManager = new RedisClusterManager("test123", "node-2", operations);
            clusterManager.startup();
            Thread.sleep(1000);
            eventBus.addBroker(new RedisClusterEventBroker(clusterManager, operations.getConnectionFactory()));

            broker2 = new EventBusDeviceOperationBroker("node-2", eventBus);
            broker2.start();
        }

    }

    @Test
    @SneakyThrows
    public void testCluster() {
        doTestStateChecker(broker1, broker2);
//        doTestSendMessage(broker1, broker2);
//        doTestSendTwiceMessage(broker1, broker2);
//        doTestSendFragMessage(broker1, broker2);
    }


    @SneakyThrows
    public void doTestStateChecker(EventBusDeviceOperationBroker broker1, EventBusDeviceOperationBroker broker2) {

        Disposable disposable = broker1.handleGetDeviceState("node-1", request -> {
            return Flux.from(request)
                       .map(id -> new DeviceStateInfo(id, DeviceState.online));
        });

        broker2.getDeviceState("node-1", Collections.singleton("test"))
               .as(StepVerifier::create)
               .expectNextMatches(info -> info.getDeviceId().equals("test") && info.getState() == DeviceState.online)
               .verifyComplete();
        disposable.dispose();

    }

    @SneakyThrows
    public void doTestSendTwiceMessage(EventBusDeviceOperationBroker broker1, EventBusDeviceOperationBroker broker2) {
        Disposable disposable = broker1.handleSendToDeviceMessage("node-1")
                                       .cast(RepayableDeviceMessage.class)
                                       .flatMap(msg -> {
                                           return broker1.reply(msg.newReply().success())
                                                         .delayElement(Duration.ofSeconds(1))
                                                         .then(broker1.reply(msg.newReply().success()));
                                       })
                                       .subscribe();

        ReadPropertyMessage message = new ReadPropertyMessage();
        message.setDeviceId("test");
        message.setMessageId(IDGenerator.UUID.generate());

        Flux<DeviceMessageReply> handle = broker2.handleReply(message.getDeviceId(), message.getMessageId(), Duration.ofSeconds(10));

        broker2.send("node-1", Mono.just(message))
               .thenMany(handle)
               .map(DeviceMessageReply::isSuccess)
               .as(StepVerifier::create)
               .expectNext(true)
               .verifyComplete();
        disposable.dispose();
    }

    @SneakyThrows
    public void doTestSendMessage(EventBusDeviceOperationBroker broker1, EventBusDeviceOperationBroker broker2) {
        Disposable disposable = broker1.handleSendToDeviceMessage("node-1")
                                       .cast(RepayableDeviceMessage.class)
                                       .flatMap(msg -> broker1.reply(msg.newReply().success()))
                                       .subscribe();

        ReadPropertyMessage message = new ReadPropertyMessage();
        message.setDeviceId("test");
        message.setMessageId(IDGenerator.UUID.generate());

        Flux<DeviceMessageReply> handle = broker2.handleReply(message.getDeviceId(), message.getMessageId(), Duration.ofSeconds(10));

        broker2.send("node-1", Mono.just(message))
               .thenMany(handle)
               .map(DeviceMessageReply::isSuccess)
               .as(StepVerifier::create)
               .expectNext(true)
               .verifyComplete();
        disposable.dispose();
    }

    @SneakyThrows
    public void doTestSendFragMessage(EventBusDeviceOperationBroker broker1, EventBusDeviceOperationBroker broker2) {
        Disposable disposable = broker1.handleSendToDeviceMessage("node-1")
                                       .cast(RepayableDeviceMessage.class)
                                       .flatMap(msg -> Flux
                                               .range(0, 10)
                                               .map(i -> {
                                                   return msg.newReply().success()
                                                             .addHeader(Headers.fragmentNumber, 10)
                                                             .addHeader(Headers.fragmentBodyMessageId, msg.getMessageId())
                                                             .addHeader(Headers.fragmentPart, i)
                                                             .messageId(IDGenerator.UUID.generate());
                                               })
                                               .flatMap(broker1::reply)
                                               .then())
                                       .subscribe();

        ReadPropertyMessage message = new ReadPropertyMessage();
        message.setDeviceId("test");
        message.setMessageId(IDGenerator.UUID.generate());

        Flux<DeviceMessageReply> handle = broker2.handleReply(message.getDeviceId(), message.getMessageId(), Duration.ofSeconds(10));

        broker2.send("node-1", Mono.just(message))
               .thenMany(handle)
               .as(StepVerifier::create)
               .expectNextCount(10)
               .verifyComplete();
        disposable.dispose();
    }

}