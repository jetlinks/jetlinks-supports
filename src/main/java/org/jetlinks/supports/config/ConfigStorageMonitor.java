package org.jetlinks.supports.config;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

/**
 * Manager-scoped capacity and fault observations. Successful reads and loads do no
 * monitoring work. Errors retain only their stage, type and time, never business values,
 * keys or Throwable graphs; snapshots use standard JMX OpenData.
 */
final class ConfigStorageMonitor {

    private static volatile CompositeType statisticsType;

    final LongAdder loadErrors = new LongAdder();
    final LongAdder staleFallbacks = new LongAdder();
    final LongAdder invalidationErrors = new LongAdder();
    final LongAdder listenerErrors = new LongAdder();
    private final String instanceId = UUID.randomUUID().toString();
    private volatile ErrorSnapshot lastError = new ErrorSnapshot("", "", 0);

    // Includes loading/result-delivery errors and local cache conflicts, not Redis request counts.
    void loadError(Throwable error) {
        loadErrors.increment();
        error("load", error);
    }

    void invalidationError(Throwable error) {
        invalidationErrors.increment();
        error("invalidation", error);
    }

    void listenerError(Throwable error) {
        listenerErrors.increment();
        error("listener", error);
    }

    void error(String stage, Throwable error) {
        lastError = new ErrorSnapshot(stage, error.getClass().getName(), System.currentTimeMillis());
    }

    CompositeData snapshot(Map<String, Object> values) {
        values.put("schemaVersion", 1L);
        values.put("instanceId", instanceId);
        values.put("capturedAt", System.currentTimeMillis());
        values.put("loadErrors", loadErrors.sum());
        values.put("staleFallbacks", staleFallbacks.sum());
        values.put("invalidationErrors", invalidationErrors.sum());
        values.put("listenerErrors", listenerErrors.sum());
        // Read one immutable snapshot so concurrent failures cannot mix stages and types.
        ErrorSnapshot error = lastError;
        values.put("lastErrorStage", error.stage);
        values.put("lastErrorType", error.type);
        values.put("lastErrorAt", error.at);
        CompositeType type = statisticsType;
        if (type == null) {
            statisticsType = type = createType("ConfigStorageStatistics", values);
        }
        return openData(type, values);
    }

    private static final class ErrorSnapshot {
        final String stage;
        final String type;
        final long at;

        ErrorSnapshot(String stage, String type, long at) {
            this.stage = stage;
            this.type = type;
            this.at = at;
        }
    }

    // Fixed schemas contain only field names/types, never runtime values or business keys.
    static CompositeType createType(String name, Map<String, Object> values) {
        String[] names = values.keySet().toArray(new String[0]);
        OpenType<?>[] types = new OpenType<?>[names.length];
        for (int i = 0; i < names.length; i++) {
            Object value = values.get(names[i]);
            types[i] = value instanceof String ? SimpleType.STRING
                : value instanceof Boolean ? SimpleType.BOOLEAN : SimpleType.LONG;
        }
        try {
            return new CompositeType(name, name, names, names, types);
        } catch (OpenDataException error) {
            throw new IllegalStateException("Invalid config storage statistics schema", error);
        }
    }

    static CompositeData openData(CompositeType type, Map<String, Object> values) {
        try {
            return new CompositeDataSupport(type, values);
        } catch (OpenDataException error) {
            throw new IllegalStateException("Invalid config storage statistics values", error);
        }
    }

}
