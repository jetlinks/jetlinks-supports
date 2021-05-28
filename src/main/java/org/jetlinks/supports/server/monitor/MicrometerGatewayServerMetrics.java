package org.jetlinks.supports.server.monitor;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.jetlinks.core.server.monitor.GatewayServerMetrics;
import org.jetlinks.core.server.session.DeviceSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MicrometerGatewayServerMetrics implements GatewayServerMetrics {

    private final String serverId;

    private final MeterRegistry registry;

    private final Map<String, AtomicLong> sessionRecord = new ConcurrentHashMap<>();

    public MicrometerGatewayServerMetrics(String serverId) {
        this(serverId, null);
    }

    public MicrometerGatewayServerMetrics(String serverId, MeterRegistry registry) {
        this.serverId = serverId;

        if (registry == null) {
            registry = Metrics.globalRegistry;
        }
        this.registry = registry;

        Gauge.builder("gateway-server-session", sessionRecord, sessionRecord -> sessionRecord
                .values()
                .stream()
                .mapToLong(Number::longValue)
                .sum())
             .tag("server", getServerId())
             .register(registry);
    }

    public String getServerId() {
        return serverId;
    }

    @Override
    public void reportSession(String transport, int sessionTotal) {
        sessionRecord
                .computeIfAbsent(transport, key -> new AtomicLong())
                .set(sessionTotal);
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
                .increment(1);
    }
}
