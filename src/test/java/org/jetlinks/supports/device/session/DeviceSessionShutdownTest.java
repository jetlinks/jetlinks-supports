package org.jetlinks.supports.device.session;

import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.ProductInfo;
import org.jetlinks.core.device.session.DeviceSessionEvent;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.RpcService;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.LostDeviceSession;
import org.jetlinks.supports.test.InMemoryDeviceRegistry;
import org.junit.Test;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DeviceSessionShutdownTest {
    @Test
    public void shutdownDoesNotRegisterNewOrPendingConnections() {
        try (Fixture fixture = new Fixture()) {
            fixture.one.shutdown();
            for (boolean creator : List.of(true, false)) {
                DeviceSession session = mock(DeviceSession.class);
                when(session.getDeviceId()).thenReturn("device");
                assertNull((creator
                    ? fixture.one.compute("device", Mono.just(session), Mono::just)
                    : fixture.one.compute("device", ignored -> Mono.just(session))).block());
                verify(session).close();
                assertEquals(Long.valueOf(0), fixture.one.totalSessions(true).block());
            }
            assertNull(fixture.device.getOnlineTime().block());
        }
    }

    @Test
    public void shutdownOfTheLastConnectionRecordsOfflineAndReconnectStartsANewPeriod() {
        try (Fixture fixture = new Fixture()) {
            DeviceSession first = fixture.one.connect(fixture.device, 1000);
            assertEquals(Long.valueOf(1000), fixture.device.getOnlineTime().block());
            fixture.one.shutdownConnection(first);
            assertEquals(Byte.valueOf(DeviceState.offline), fixture.device.getState().block());
            assertNotNull(fixture.device.getOfflineTime().block());
            DeviceSessionEvent closed = fixture.one.events.get(fixture.one.events.size() - 1);
            assertEquals(DeviceSessionEvent.Type.unregister, closed.getType());
            assertFalse(closed.isClusterExists());
            long offlineAt = fixture.device.getOfflineTime().block();
            assertTrue(offlineAt >= closed.getTimestamp());
            long reconnectedAt = offlineAt + 1;
            fixture.two.connect(fixture.device, reconnectedAt);
            assertEquals(Byte.valueOf(DeviceState.online), fixture.device.getState().block());
            assertEquals(Long.valueOf(reconnectedAt), fixture.device.getOnlineTime().block());
        }
    }

    @Test
    public void anotherConnectedNodePreservesTheOriginalOnlinePeriod() {
        try (Fixture fixture = new Fixture()) {
            DeviceSession first = fixture.one.connect(fixture.device, 1000);
            DeviceSession second = fixture.two.connect(fixture.device, 2000);
            fixture.one.shutdownConnection(first);
            assertEquals(Byte.valueOf(DeviceState.online), fixture.device.getState().block());
            assertEquals(Long.valueOf(1000), fixture.device.getOnlineTime().block());
            assertNull(fixture.device.getOfflineTime().block());
            assertTrue(fixture.one.events.get(fixture.one.events.size() - 1).isClusterExists());
            fixture.two.shutdownConnection(second);
            assertEquals(Byte.valueOf(DeviceState.offline), fixture.device.getState().block());
            assertFalse(fixture.two.events.get(fixture.two.events.size() - 1).isClusterExists());
        }
    }

    private static final class Fixture implements AutoCloseable {
        final DeviceOperator device;
        ClosingManager one;
        ClosingManager two;

        Fixture() {
            var registry = InMemoryDeviceRegistry.create();
            registry.register(ProductInfo.builder().id("product").protocol("test").metadata("{}").build()).block();
            device = registry.register(DeviceInfo.builder().id("device").productId("product").build()).block();
            one = new ClosingManager(rpc("one", "two", () -> two));
            two = new ClosingManager(rpc("two", "one", () -> one));
            one.init();
            two.init();
        }

        public void close() {
            one.shutdown();
            two.shutdown();
        }
    }

    private static RpcManager rpc(String local, String remote, Supplier<ClosingManager> other) {
        RpcManager rpc = mock(RpcManager.class);
        when(rpc.currentServerId()).thenReturn(local);
        when(rpc.registerService(any())).thenReturn(Disposables.single());
        var service = new ClusterDeviceSessionManager.ServiceImpl(other::get);
        when(rpc.getServices(ClusterDeviceSessionManager.Service.class)).thenReturn(Flux.just(new RpcService<ClusterDeviceSessionManager.Service>() {
            public String serverNodeId() { return remote; }
            public String id() { return "sessions"; }
            public String name() { return "sessions"; }
            public ClusterDeviceSessionManager.Service service() { return service; }
        }));
        when(rpc.listen(ClusterDeviceSessionManager.Service.class)).thenReturn(Flux.never());
        return rpc;
    }

    private static final class ClosingManager extends ClusterDeviceSessionManager {
        final List<DeviceSessionEvent> events = new ArrayList<>();

        ClosingManager(RpcManager rpc) {
            super(rpc);
            listenEvent(event -> Mono.fromRunnable(() -> events.add(event)));
        }

        DeviceSession connect(DeviceOperator device, long connectedAt) {
            var session = new LostDeviceSession("session", device, DefaultTransport.TCP, connectedAt) {
                private boolean alive = true;
                @Override public boolean isAlive() { return alive; }
                @Override public Mono<Boolean> isAliveAsync() { return Mono.fromSupplier(this::isAlive); }
                @Override public void close() { alive = false; }
            };
            return compute(device.getDeviceId(), Mono.just(session), Mono::just).block();
        }

        void shutdownConnection(DeviceSession session) {
            shutdown();
            localSessions.clear();
            // PersistenceDeviceSessionManager drains connections after marking the manager as stopped.
            closeSessionSafe(session).block();
        }
    }
}
