package org.jetlinks.supports.cluster;

import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.DeviceStateInfo;
import org.jetlinks.core.device.StandaloneDeviceMessageBroker;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.supports.cluster.redis.RedisClusterManager;
import org.jetlinks.supports.cluster.redis.TestApplication;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;

@SpringBootTest(classes = TestApplication.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class ClusterDeviceOperationBrokerTest {

    @Autowired
    private ReactiveRedisTemplate<Object, Object> operations;

    public RedisClusterManager clusterManager;

    private ClusterDeviceOperationBroker broker;

    @Before
    public void init() {
        clusterManager = new RedisClusterManager("default","test", operations);
        broker=new ClusterDeviceOperationBroker(clusterManager);
    }


    @Test
    public void testStateCheck() {
        broker.handleGetDeviceState("test", list ->
                Flux.from(list)
                        .map(id -> new DeviceStateInfo(id, DeviceState.online)));

        broker.getDeviceState("test", Collections.singletonList("testId"))
                .map(DeviceStateInfo::getState)
                .as(StepVerifier::create)
                .expectNext(DeviceState.online)
                .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void test() {
        broker.handleSendToDeviceMessage("test")
                .subscribe(msg -> {
                    broker.reply(new FunctionInvokeMessageReply().from(msg).success())
                            .subscribe();
                });

        FunctionInvokeMessage message = new FunctionInvokeMessage();
        message.setFunctionId("test");
        message.setMessageId("test");

        Flux<Boolean> successReply = broker.handleReply(message.getMessageId(), Duration.ofSeconds(10))
                .map(DeviceMessageReply::isSuccess);

        broker.send("test", Mono.just(message))
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

        successReply.as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    public void testParting() {
        StandaloneDeviceMessageBroker handler = new StandaloneDeviceMessageBroker();
        handler.handleSendToDeviceMessage("test")
                .subscribe(msg -> {
                    handler.reply(new FunctionInvokeMessageReply()
                            .from(msg)
                            .addHeader(Headers.fragmentBodyMessageId, msg.getMessageId())
                            .addHeader(Headers.fragmentNumber, 2)
                            .messageId("2")
                            .success())
                            .delayElement(Duration.ofSeconds(1))
                            .flatMap(success ->
                                    handler.reply(new FunctionInvokeMessageReply()
                                            .from(msg)
                                            .messageId("1")
                                            .addHeader(Headers.fragmentBodyMessageId, msg.getMessageId())
                                            .addHeader(Headers.fragmentNumber, 2)
                                            .success()))
                            .subscribe();
                });

        FunctionInvokeMessage message = new FunctionInvokeMessage();
        message.setFunctionId("test");
        message.setMessageId("test");

        Flux<Boolean> successReply = handler
                .handleReply(message.getMessageId(), Duration.ofSeconds(2))
                .doOnNext(System.out::println)
                .map(DeviceMessageReply::isSuccess);

        handler.send("test", Mono.just(message))
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

        successReply.
                as(StepVerifier::create)
                .expectNext(true, true)
                .verifyComplete();
    }

}