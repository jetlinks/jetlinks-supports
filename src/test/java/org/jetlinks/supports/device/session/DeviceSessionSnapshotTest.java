package org.jetlinks.supports.device.session;

import org.jetlinks.core.device.session.DeviceConnectionInfo;
import org.jetlinks.core.device.session.DeviceSessionInfo;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.RpcService;
import org.jetlinks.core.rpc.ServiceEvent;
import org.jetlinks.core.server.ConnectionMetricsInfo;
import org.junit.Test;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class DeviceSessionSnapshotTest {
    @Test
    public void successfulEmptyNodesAreCompleteAndLocalOnlyManagerProvesLocalCoverage() {
        try (var fixture = new Fixture()) {
            var result = fixture.manager.getDeviceSessionSnapshot("device").block();
            assertTrue(result.isComplete());
            assertTrue(result.getSessions().isEmpty());
            assertEquals(Set.of("one", "two"), result.getRequestedServerIds());
        }
        var local = LocalDeviceSessionManager.create();
        var result = local.getDeviceSessionSnapshot("device").block();
        assertTrue(result.isComplete());
        assertEquals(Set.of("local"), result.getRequestedServerIds());
        assertNull(local.getDeviceSessionSnapshot(" ").block());
    }

    @Test
    public void sameDeviceOnTwoNodesContributesEachConnectionOnce() {
        try (var fixture = new Fixture()) {
            fixture.local.set(Flux.just(info("one", 10, 20)));
            when(fixture.remote.deviceSessions("device")).thenReturn(Flux.just(info("two", 30)));
            var result = fixture.manager.getDeviceSessionSnapshot("device").block();
            assertTrue(result.isComplete());
            assertEquals(2, result.getSessions().size());
            assertEquals(3, result.getSessions().stream().mapToInt(item -> item.getConnections().size()).sum());
            assertEquals(60, ConnectionMetricsInfo.aggregate(result.getSessions(), DeviceSessionInfo::getMetrics).getReadBytes());
            verify(fixture.remote).deviceSessions("device");
        }
    }

    @Test
    public void remoteFailureAndFailureAfterAnItemCannotBecomeSuccessfulAbsence() {
        try (var fixture = new Fixture()) {
            fixture.local.set(Flux.just(info("one", 10)));
            when(fixture.remote.deviceSessions("device")).thenReturn(Flux.concat(
                Flux.just(info("two", 30)), Flux.error(new IllegalStateException("failed after item"))));
            var result = fixture.manager.getDeviceSessionSnapshot("device").block();
            assertFalse(result.isComplete());
            assertEquals(Set.of("two"), result.getFailedServerIds());
            assertEquals(1, result.getSessions().size());
            assertEquals("one", result.getSessions().get(0).getServerId());
            // Existing best-effort readers retain their published behavior.
            assertEquals(2, fixture.manager.getDeviceSessionInfo("device").collectList().block().size());
        }
    }

    @Test
    public void malformedIdentityDuplicateRowsAndSynchronousFailuresAreNodeFailures() {
        try (var fixture = new Fixture()) {
            when(fixture.remote.deviceSessions("device")).thenReturn(Flux.just(info("other", 10)));
            assertEquals(Set.of("two"), fixture.manager.getDeviceSessionSnapshot("device").block().getFailedServerIds());
            when(fixture.remote.deviceSessions("device")).thenReturn(Flux.just(info("two", 10), info("two", 20)));
            assertFalse(fixture.manager.getDeviceSessionSnapshot("device").block().isComplete());
            when(fixture.remote.deviceSessions("device")).thenThrow(new IllegalStateException("unavailable"));
            assertEquals(Set.of("two"), fixture.manager.getDeviceSessionSnapshot("device").block().getFailedServerIds());
        }
    }

    @Test
    public void cancellationStopsPendingRemoteReadAndContextIsPreserved() {
        try (var fixture = new Fixture()) {
            var cancelled = new AtomicBoolean();
            var subscribed = new AtomicBoolean();
            when(fixture.remote.deviceSessions("device")).thenReturn(Flux.deferContextual(context -> {
                assertEquals("value", context.get("test"));
                subscribed.set(true);
                return Flux.<DeviceSessionInfo>never().doOnCancel(() -> cancelled.set(true));
            }));
            var read = fixture.manager.getDeviceSessionSnapshot("device")
                .contextWrite(context -> context.put("test", "value")).subscribe();
            try {
                assertTrue(subscribed.get());
            } finally {
                read.dispose();
            }
            assertTrue(cancelled.get());
        }
    }

    @Test
    public void serviceRemovedDuringReadRemainsRequestedUntilNextSubscription() {
        try (var fixture = new Fixture()) {
            when(fixture.remote.deviceSessions("device")).thenReturn(Flux.defer(() -> {
                fixture.events.tryEmitNext(new ServiceEvent("sessions", "sessions", "two", ServiceEvent.Type.removed));
                return Flux.error(new IllegalStateException("removed during read"));
            }));
            var first = fixture.manager.getDeviceSessionSnapshot("device").block();
            assertEquals(Set.of("one", "two"), first.getRequestedServerIds());
            assertEquals(Set.of("two"), first.getFailedServerIds());
            var second = fixture.manager.getDeviceSessionSnapshot("device").block();
            assertTrue(second.isComplete());
            assertEquals(Set.of("one"), second.getRequestedServerIds());
        }
    }

    private static DeviceSessionInfo info(String server, long... bytes) {
        var result = new DeviceSessionInfo();
        result.setDeviceId("device");
        result.setServerId(server);
        result.setConnections(java.util.Arrays.stream(bytes).mapToObj(value -> {
            var connection = new DeviceConnectionInfo();
            connection.setMetrics(new ConnectionMetricsInfo(value, 0, 1, 2, 0, 0, 0));
            return connection;
        }).toList());
        return result;
    }

    private static final class Fixture implements AutoCloseable {
        final RpcManager rpc = mock(RpcManager.class);
        final ClusterDeviceSessionManager.Service remote = mock(ClusterDeviceSessionManager.Service.class);
        final Sinks.Many<ServiceEvent> events = Sinks.many().unicast().onBackpressureBuffer();
        final AtomicReference<Flux<DeviceSessionInfo>> local = new AtomicReference<>(Flux.empty());
        final ClusterDeviceSessionManager manager;

        Fixture() {
            when(rpc.currentServerId()).thenReturn("one");
            when(rpc.registerService(any())).thenReturn(Disposables.single());
            when(rpc.getServices(ClusterDeviceSessionManager.Service.class)).thenReturn(Flux.just(new RpcService<>() {
                public String serverNodeId() { return "two"; }
                public String id() { return "sessions"; }
                public String name() { return "sessions"; }
                public ClusterDeviceSessionManager.Service service() { return remote; }
            }));
            when(rpc.listen(ClusterDeviceSessionManager.Service.class)).thenReturn(events.asFlux());
            when(remote.deviceSessions(anyString())).thenReturn(Flux.empty());
            manager = new ClusterDeviceSessionManager(rpc) {
                @Override
                protected Flux<DeviceSessionInfo> getLocalDeviceSessionInfo(String deviceId) {
                    return local.get();
                }
            };
            manager.init();
        }

        public void close() {
            manager.shutdown();
            events.tryEmitComplete();
        }
    }
}
