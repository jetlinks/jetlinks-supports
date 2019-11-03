package org.jetlinks.supports;

import lombok.AllArgsConstructor;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

@AllArgsConstructor
public class TestDeviceSession implements DeviceSession {

    private String deviceId;

    private DeviceOperator operator;

    private Consumer<EncodedMessage> consumer;

    @Override
    public String getId() {
        return deviceId;
    }

    @Override
    public String getDeviceId() {
        return deviceId;
    }

    @Override
    public DeviceOperator getOperator() {
        return operator;
    }

    @Override
    public long lastPingTime() {
        return 0;
    }

    @Override
    public long connectTime() {
        return 0;
    }

    @Override
    public Mono<Boolean> send(EncodedMessage encodedMessage) {
        return Mono.fromSupplier(() -> {
            consumer.accept(encodedMessage);
            return true;
        });
    }

    @Override
    public Transport getTransport() {
        return DefaultTransport.MQTT;
    }

    @Override
    public void close() {

    }

    @Override
    public void ping() {

    }

    @Override
    public boolean isAlive() {
        return true;
    }

    @Override
    public void onClose(Runnable call) {

    }
}
