package org.jetlinks.supports.server.session;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import org.jetlinks.core.device.*;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.server.monitor.GatewayServerMetrics;
import org.jetlinks.core.server.monitor.GatewayServerMonitor;
import org.jetlinks.supports.DefaultProtocolSupports;
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
        DeviceRegistry registry = new TestDeviceRegistry(new DefaultProtocolSupports(), new StandaloneDeviceMessageBroker());

        DefaultDeviceSessionManager sessionManager = new DefaultDeviceSessionManager();
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

        Metrics.addRegistry(new SimpleMeterRegistry());
        Metrics.addRegistry(new LoggingMeterRegistry());

        sessionManager.init();

        Flux.range(0, 50_000)
                .map(i -> DeviceInfo.builder()
                        .id("test_" + i)
                        .protocol("test")
                        .build())
                .publishOn(Schedulers.parallel())
                .flatMap(registry::registry)
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