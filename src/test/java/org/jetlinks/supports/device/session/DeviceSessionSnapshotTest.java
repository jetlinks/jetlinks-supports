package org.jetlinks.supports.device.session;

import org.jetlinks.core.device.session.DeviceConnectionInfo;
import org.jetlinks.core.device.session.DeviceSessionInfo;
import org.jetlinks.core.device.session.DeviceSessionStatistics;
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
    public void localOnlyClusterReadsOncePerSubscriptionAndNeverCallsItsOwnRpc() {
        for (String[] servers : new String[][]{new String[0], {"one"}}) {
            try (var fixture = new Fixture(servers)) {
                var subscriptions = new java.util.concurrent.atomic.AtomicInteger();
                fixture.local.set(Flux.defer(() -> {
                    subscriptions.incrementAndGet();
                    return Flux.just(info("one", 10));
                }));
                var pending = fixture.manager.getDeviceSessionSnapshot("device");
                assertEquals(0, subscriptions.get());
                var result = pending.block();
                assertTrue(result.isComplete());
                assertEquals(Set.of("one"), result.getRequestedServerIds());
                assertEquals(1, result.getSessions().size());
                pending.block();
                assertEquals(2, subscriptions.get());
                verify(fixture.remote, never()).deviceSessions(anyString());
            }
        }
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
    public void duplicateRowsAndSynchronousFailuresAreNodeFailures() {
        try (var fixture = new Fixture()) {
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

    @Test
    public void statisticsRpcUsesOnlyLocalAggregatesAndMergesEveryNodeOnce() {
        try (var fixture = new Fixture()) {
            fixture.local.set(Flux.error(new AssertionError("must not read session details")));
            fixture.localStatistics.set(Mono.just(statistics(10, 20)));
            when(fixture.remote.deviceStatistics("device")).thenReturn(Mono.just(statistics(30)));
            var local = new ClusterDeviceSessionManager.ServiceImpl(() -> fixture.manager).deviceStatistics("device").block();
            assertEquals(30, local.getMetrics().getReadBytes());
            verify(fixture.remote, never()).deviceStatistics(anyString());
            var result = fixture.manager.getDeviceSessionStatistics("device").block();
            assertTrue(result.isComplete());
            assertEquals(Set.of("one", "two"), result.getRequestedServerIds());
            assertEquals(2, result.getSessionCount());
            assertEquals(Long.valueOf(3), result.getConnectionCount());
            assertEquals(60, result.getMetrics().getReadBytes());
            verify(fixture.remote).deviceStatistics("device");
            verify(fixture.remote, never()).deviceSessions(anyString());
        }
    }

    @Test
    public void statisticsDistinguishZeroSessionsFromFailedEmptyAndOldEndpoints() {
        try (var fixture = new Fixture()) {
            var complete = fixture.manager.getDeviceSessionStatistics("device").block();
            assertTrue(complete.isComplete());
            assertEquals(0, complete.getSessionCount());
            assertEquals(Long.valueOf(0), complete.getConnectionCount());
            fixture.localStatistics.set(Mono.just(statistics(10)));
            for (Mono<DeviceSessionStatistics> failure : List.of(
                Mono.<DeviceSessionStatistics>empty(),
                Mono.<DeviceSessionStatistics>error(new UnsupportedOperationException("old endpoint")),
                Mono.fromDirect(Flux.concat(Flux.just(statistics(30)), Flux.error(new IllegalStateException("failed after item")))))) {
                when(fixture.remote.deviceStatistics("device")).thenReturn(failure);
                var result = fixture.manager.getDeviceSessionStatistics("device").block();
                assertFalse(result.isComplete());
                assertEquals(Set.of("two"), result.getFailedServerIds());
                assertEquals(1, result.getSessionCount());
                assertEquals(10, result.getMetrics().getReadBytes());
            }
            when(fixture.remote.deviceStatistics("device")).thenThrow(new IllegalStateException("synchronous failure"));
            assertFalse(fixture.manager.getDeviceSessionStatistics("device").block().isComplete());
        }
    }

    @Test
    public void statisticsCancellationAndContextReachTheRemoteRead() {
        try (var fixture = new Fixture()) {
            var subscribed = new AtomicBoolean();
            var cancelled = new AtomicBoolean();
            when(fixture.remote.deviceStatistics("device")).thenReturn(Mono.deferContextual(context -> {
                assertEquals("value", context.get("test"));
                subscribed.set(true);
                return Mono.<DeviceSessionStatistics>never().doOnCancel(() -> cancelled.set(true));
            }));
            var read = fixture.manager.getDeviceSessionStatistics("device")
                .contextWrite(context -> context.put("test", "value")).subscribe();
            try { assertTrue(subscribed.get()); }
            finally { read.dispose(); }
            assertTrue(cancelled.get());
        }
    }

    @Test
    public void statisticsServiceRemovalDoesNotRewriteInFlightCoverage() {
        try (var fixture = new Fixture()) {
            when(fixture.remote.deviceStatistics("device")).thenReturn(Mono.defer(() -> {
                fixture.events.tryEmitNext(new ServiceEvent("sessions", "sessions", "two", ServiceEvent.Type.removed));
                return Mono.error(new IllegalStateException("removed during read"));
            }));
            var first = fixture.manager.getDeviceSessionStatistics("device").block();
            assertEquals(Set.of("one", "two"), first.getRequestedServerIds());
            assertEquals(Set.of("two"), first.getFailedServerIds());
            var second = fixture.manager.getDeviceSessionStatistics("device").block();
            assertTrue(second.isComplete());
            assertEquals(Set.of("one"), second.getRequestedServerIds());
        }
    }

    @Test
    public void localStatisticsReadPerSubscriptionWithoutSelfRpc() {
        var local = LocalDeviceSessionManager.create();
        assertTrue(local.getDeviceSessionStatistics("device").block().isComplete());
        assertNull(local.getDeviceSessionStatistics(" ").block());
        for (String[] servers : new String[][]{new String[0], {"one"}}) {
            try (var fixture = new Fixture(servers)) {
                var subscriptions = new java.util.concurrent.atomic.AtomicInteger();
                fixture.localStatistics.set(Mono.fromSupplier(() -> {
                    subscriptions.incrementAndGet();
                    return statistics(10);
                }));
                var pending = fixture.manager.getDeviceSessionStatistics("device");
                assertEquals(0, subscriptions.get());
                assertTrue(pending.block().isComplete());
                pending.block();
                assertEquals(2, subscriptions.get());
                verify(fixture.remote, never()).deviceStatistics(anyString());
            }
        }
    }

    private static DeviceSessionStatistics statistics(long... bytes) {
        return DeviceSessionStatistics.fromSessions(List.of(info("node", bytes)));
    }

    private static final class Fixture implements AutoCloseable {
        final RpcManager rpc = mock(RpcManager.class);
        final ClusterDeviceSessionManager.Service remote = mock(ClusterDeviceSessionManager.Service.class);
        final Sinks.Many<ServiceEvent> events = Sinks.many().unicast().onBackpressureBuffer();
        final AtomicReference<Flux<DeviceSessionInfo>> local = new AtomicReference<>(Flux.empty());
        final AtomicReference<Mono<DeviceSessionStatistics>> localStatistics = new AtomicReference<>(Mono.fromSupplier(DeviceSessionStatistics::new));
        final ClusterDeviceSessionManager manager;

        Fixture() {
            this("two");
        }

        Fixture(String... servers) {
            when(rpc.currentServerId()).thenReturn("one");
            when(rpc.registerService(any())).thenReturn(Disposables.single());
            when(rpc.getServices(ClusterDeviceSessionManager.Service.class)).thenReturn(Flux.fromArray(servers).map(server -> new RpcService<ClusterDeviceSessionManager.Service>() {
                public String serverNodeId() { return server; }
                public String id() { return "sessions"; }
                public String name() { return "sessions"; }
                public ClusterDeviceSessionManager.Service service() { return remote; }
            }));
            when(rpc.listen(ClusterDeviceSessionManager.Service.class)).thenReturn(events.asFlux());
            when(remote.deviceSessions(anyString())).thenReturn(Flux.empty());
            when(remote.deviceStatistics(anyString())).thenAnswer(ignored -> Mono.just(new DeviceSessionStatistics()));
            manager = new ClusterDeviceSessionManager(rpc) {
                @Override
                protected Flux<DeviceSessionInfo> getLocalDeviceSessionInfo(String deviceId) {
                    return local.get();
                }
                @Override
                protected Mono<DeviceSessionStatistics> getLocalDeviceSessionStatistics(String deviceId) {
                    return localStatistics.get();
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
