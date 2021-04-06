package org.jetlinks.supports.test;

import org.jetlinks.core.Value;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.ProductInfo;
import org.junit.Test;
import reactor.test.StepVerifier;

import static org.junit.Assert.*;

public class InMemoryDeviceRegistryTest {

    @Test
    public void test() {

        InMemoryDeviceRegistry registry = InMemoryDeviceRegistry.create();

        registry.register(ProductInfo
                                  .builder()
                                  .id("test")
                                  .protocol("test")
                                  .build())

                .then(
                        registry.register(DeviceInfo.builder()
                                                    .productId("test")
                                                    .id("test")
                                                    .build())
                )
                .then(registry.getDevice("test"))
                .flatMap(device -> device.setConfig("test", "1234").thenReturn(device))
                .flatMap(device -> device.getSelfConfig("test").map(Value::asString))
                .as(StepVerifier::create)
                .expectNext("1234")
                .verifyComplete();


    }
}