package org.jetlinks.supports.server.session;

import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperationBroker;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.StandaloneDeviceMessageBroker;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.server.monitor.GatewayServerMetrics;
import org.jetlinks.core.server.monitor.GatewayServerMonitor;
import org.jetlinks.supports.protocol.StaticProtocolSupports;
import org.jetlinks.supports.server.monitor.MicrometerGatewayServerMetrics;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.stream.Collectors;

public class DefaultDeviceSessionManagerTest {


    private DeviceOperationBroker handler = new StandaloneDeviceMessageBroker();


    @Test
    @SneakyThrows
    public void test() {
        DeviceRegistry registry = new TestDeviceRegistry(new StaticProtocolSupports(), new StandaloneDeviceMessageBroker());

        DefaultDeviceSessionManager sessionManager = new DefaultDeviceSessionManager();
        sessionManager.setRegistry(registry);

        sessionManager.setGatewayServerMonitor(new GatewayServerMonitor() {
            @Override
            public String getCurrentServerId() {
                return "test";
            }

            @Override
            public GatewayServerMetrics metrics() {
                return new MicrometerGatewayServerMetrics("test");
            }
        });

        sessionManager.init();

        Flux.range(0, 50_000)
                .map(i -> DeviceInfo.builder()
                        .id("test_" + i)
                        .protocol("test")
                        .build())
                .publishOn(Schedulers.parallel())
                .flatMap(registry::register)
                .doOnNext(deviceOperator -> {
                    sessionManager.register(new TestDeviceSession(DefaultTransport.MQTT, deviceOperator.getDeviceId(), deviceOperator.getDeviceId(), deviceOperator));

                }).collect(Collectors.counting())
                .as(StepVerifier::create)
                .expectNext(50_000L)
                .verifyComplete();


        sessionManager.checkSession()
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();

        Thread.sleep(1000);


    }
}