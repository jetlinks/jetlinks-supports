package org.jetlinks.supports.server.session;

import lombok.Getter;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class TestDeviceSession implements DeviceSession {

    @Getter
    private String id;

    @Getter
    private String deviceId;

    @Getter
    private DeviceOperator operator;

    @Getter
    private Transport transport;

    public TestDeviceSession(Transport transport, String id, String deviceId, DeviceOperator operator) {
        this.transport = transport;
        this.id = id;
        this.deviceId = deviceId;
        this.operator = operator;
    }

    private Function<EncodedMessage, Mono<Boolean>> sender = (msg) -> Mono.just(true);

    private long lastPingTime = System.currentTimeMillis();

    @Override
    public DeviceOperator getOperator() {
        return operator;
    }

    @Override
    public long lastPingTime() {
        return lastPingTime;
    }

    @Override
    public long connectTime() {
        return 0;
    }

    @Override
    public Mono<Boolean> send(EncodedMessage encodedMessage) {
        return sender.apply(encodedMessage);
    }


    @Override
    public void close() {

    }

    @Override
    public void ping() {
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        return System.currentTimeMillis() - lastPingTime < 10000;
    }

    @Override
    public void onClose(Runnable call) {

    }
}
