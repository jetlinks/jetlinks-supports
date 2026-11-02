package org.jetlinks.supports.config;

import org.jetlinks.core.cluster.ClusterCache;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.event.InternalEventBus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConfigStorageMonitorTest {

    private ClusterManager clusterManager;
    private ClusterCache<String, Object> clusterCache;
    private EventBusStorageManager manager;
    private EventBusStorageManager.ConfigStorageManagerMbean mbean;
    private Map<String, LocalCacheClusterConfigStorage.Cache> entries;
    private LocalCacheClusterConfigStorage storage;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        clusterManager = mock(ClusterManager.class);
        clusterCache = mock(ClusterCache.class);
        manager = new EventBusStorageManager(clusterManager, new InternalEventBus());
        mbean = manager.new ConfigStorageManagerMbeanImpl();
        entries = new ConcurrentHashMap<>();
        storage = new LocalCacheClusterConfigStorage("test", manager, clusterCache, 60_000, null, entries);
        manager.cache.put("test", storage);
    }

    @After
    public void tearDown() {
        manager.dispose();
    }

    @Test
    public void countsIncludeNegativeExpiredAndLoadingEntriesWithoutReadingValues() {
        when(clusterCache.get("value")).thenReturn(Mono.just("secret"));
        when(clusterCache.get("missing")).thenReturn(Mono.empty());
        when(clusterCache.get("expired")).thenReturn(Mono.just("old"));
        Sinks.One<Object> pending = Sinks.one();
        when(clusterCache.get("loading")).thenReturn(pending.asMono());
        storage.getConfig("value").block();
        storage.getConfig("missing").block();
        storage.getConfig("expired").block();
        storage.getConfig("loading");
        entries.get("expired").t = System.currentTimeMillis() - 120_000;
        manager.cache.put("empty", new LocalCacheClusterConfigStorage("empty", manager, clusterCache, -1, null));
        clearInvocations(clusterCache);

        try {
            assertEquals(2, mbean.getCacheSize());
            assertEquals(4, mbean.countLocalEntries());
            Map<String, String> states = new HashMap<>();
            for (CompositeData entry : mbean.inspect("test", 128)) {
                assertEquals(4, entry.getCompositeType().keySet().size());
                assertFalse(entry.containsKey("value"));
                assertTrue(entry.get("version") instanceof Long);
                states.put((String) entry.get("key"), (String) entry.get("state"));
            }
            assertEquals("value", states.get("value"));
            assertEquals("negative", states.get("missing"));
            assertEquals("expired", states.get("expired"));
            assertEquals("loading", states.get("loading"));
            verifyNoInteractions(clusterCache, clusterManager);
        } finally {
            pending.tryEmitEmpty();
        }
    }

    @Test
    public void countingUsesMapSizeWithoutTraversingKeys() {
        Map<String, LocalCacheClusterConfigStorage.Cache> sizeOnly = new ConcurrentHashMap<String, LocalCacheClusterConfigStorage.Cache>() {
            @Override
            public java.util.Set<Map.Entry<String, LocalCacheClusterConfigStorage.Cache>> entrySet() {
                throw new AssertionError("Counting must not traverse keys");
            }
        };
        sizeOnly.put("a", storage.new Cache("a"));
        manager.cache.put("other", new LocalCacheClusterConfigStorage("other", manager, clusterCache, -1, null, sizeOnly));

        assertEquals(1, mbean.countLocalEntries());
        verifyNoInteractions(clusterCache, clusterManager);
    }

    @Test
    public void observationDoesNotRenewTtlOrChangeCacheState() {
        LocalCacheClusterConfigStorage.Cache entry = storage.new Cache("key");
        entries.put("key", entry);
        entry.setValue(entry.version, "secret");
        entry.t = 1;
        Object cached = entry.cached;
        Object ref = entry.ref;
        int version = entry.version;

        assertEquals(1, mbean.countLocalEntries());
        assertEquals("expired", mbean.inspect("test", 10)[0].get("state"));
        assertEquals(1, entry.t);
        assertSame(cached, entry.cached);
        assertSame(ref, entry.ref);
        assertEquals(version, entry.version);
        assertNull(entry.loading);
        assertEquals(1, entries.size());
        verifyNoInteractions(clusterCache, clusterManager);
    }

    @Test
    public void inspectionReportsUnloadedEntryWithoutExposingItsRetainedValue() {
        LocalCacheClusterConfigStorage.Cache entry = storage.new Cache("key");
        entries.put("key", entry);
        entry.setValue(entry.version, "secret");
        entry.clear(entry.cached);

        CompositeData info = mbean.inspect("test", 1)[0];
        assertEquals("unloaded", info.get("state"));
        assertFalse(info.toString().contains("secret"));
        verifyNoInteractions(clusterCache, clusterManager);
    }

    @Test
    public void inspectionLimitsEntriesAndKeyLengthAndDoesNotCreateStorage() {
        String longKey = String.join("", java.util.Collections.nCopies(600, "k"));
        entries.put(longKey, storage.new Cache(longKey));
        CompositeData info = mbean.inspect("test", 1)[0];
        assertEquals(512, ((String) info.get("key")).length());
        assertEquals(Boolean.TRUE, info.get("keyTruncated"));
        for (int i = 0; i < 200; i++) {
            entries.put("key-" + i, storage.new Cache("key-" + i));
        }

        assertEquals(128, mbean.inspect("test", Integer.MAX_VALUE).length);
        assertEquals(1, mbean.inspect("test", 0).length);
        assertEquals(0, mbean.inspect("unknown", 10).length);
        assertEquals(1, manager.cache.size());
        assertEquals(201, mbean.countLocalEntries());
        verifyNoInteractions(clusterCache, clusterManager);
    }

    @Test
    public void jmxExposesCountAsAnOperationAndSerializesInspection() throws Exception {
        MBeanServer server = MBeanServerFactory.newMBeanServer();
        ObjectName name = new ObjectName("org.jetlinks:type=ConfigStorage,name=test");
        server.registerMBean(new StandardMBean(mbean, EventBusStorageManager.ConfigStorageManagerMbean.class), name);
        entries.put("key", storage.new Cache("key"));

        assertEquals(1L, server.getAttribute(name, "CacheSize"));
        assertEquals(1L, server.invoke(name, "countLocalEntries", new Object[0], new String[0]));
        CompositeData[] result = (CompositeData[]) server.invoke(name, "inspect",
                                                               new Object[]{"test", 10}, new String[]{"java.lang.String", "int"});
        assertEquals("key", result[0].get("key"));
        MBeanInfo info = server.getMBeanInfo(name);
        assertTrue(Arrays.stream(info.getOperations()).anyMatch(op -> op.getName().equals("countLocalEntries")));
        assertFalse(Arrays.stream(info.getAttributes()).anyMatch(attr -> attr.getName().equals("LocalEntryCount")));
    }

    @Test
    public void mbeanRegistrationIsIdempotentAndReleasedOnDispose() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("org.jetlinks:type=ConfigStorage,name=LocalCacheClusterConfigStorage");
        assertFalse(server.isRegistered(name));

        manager.registerMbean();
        manager.registerMbean();
        assertTrue(server.isRegistered(name));
        manager.dispose();
        assertFalse(server.isRegistered(name));
        manager.registerMbean();
        assertFalse(server.isRegistered(name));
    }
}
