package org.jetlinks.supports.server.monitor;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.jetlinks.core.server.monitor.GatewayServerMetrics;
import org.jetlinks.core.server.session.DeviceSession;

public class MicrometerGatewayServerMetrics implements GatewayServerMetrics {

    private String serverId;

    private MeterRegistry registry;

    public MicrometerGatewayServerMetrics(String serverId) {
        this(serverId, null);
    }

    public MicrometerGatewayServerMetrics(String serverId, MeterRegistry registry) {
        this.serverId = serverId;

        if (registry == null) {
            registry = Metrics.globalRegistry;
        }
        this.registry = registry;
    }

    public String getServerId() {
        return serverId;
    }

    @Override
    public void reportSession(String transport, int sessionTotal) {
        Gauge.builder("gateway-server-session", sessionTotal, Number::doubleValue)
                .tag("transport", transport)
                .tag("server", getServerId())
                .description("当前" + transport + "会话数量")
                .register(registry);
    }

    @Override
    public void newConnection(String transport) {
        registry.counter("gateway-server-requests-currents", "transport", transport, "server", getServerId())
                .increment();
    }

    @Override
    public void acceptedConnection(String transport) {
        registry.counter("gateway-server-requests-currents", "transport", transport, "server", getServerId())
                .increment(-1);

        registry.counter("gateway-server-requests-accepts", "transport", transport, "server", getServerId())
                .increment();
    }

    @Override
    public void rejectedConnection(String transport) {
        registry.counter("gateway-server-requests-currents", "transport", transport, "server", getServerId())
                .increment(-1);

        registry.counter("gateway-server-requests-rejects", "transport", transport, "server", getServerId())
                .increment();
    }

    @Override
    public void receiveFromDeviceMessage(DeviceSession session) {
        registry.counter("received_messages",
                "transport", session.getTransport().getId(),
                "deviceId", session.getDeviceId(),
                "server", getServerId())
                .increment(-1);
    }
}
