package org.jetlinks.supports.cluster;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.DeviceStateChecker;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.validation.constraints.NotNull;

import static org.junit.Assert.*;

public class CompositeDeviceStateCheckerTest {

    @Test
    public void test(){
        CompositeDeviceStateChecker stateChecker=new CompositeDeviceStateChecker();

        stateChecker.addDeviceStateChecker(new DeviceStateChecker() {
            @Override
            public @NotNull Mono<Byte> checkState(@NotNull DeviceOperator device) {
                return Mono.empty();
            }

            @Override
            public long order() {
                return 0;
            }
        });

        stateChecker.addDeviceStateChecker(new DeviceStateChecker() {
            @Override
            public @NotNull Mono<Byte> checkState(@NotNull DeviceOperator device) {
                return Mono.just(DeviceState.online);
            }

            @Override
            public long order() {
                return 1;
            }
        });

        stateChecker.checkState(null)
                    .as(StepVerifier::create)
                    .expectNext(DeviceState.online)
                    .verifyComplete();

    }
}