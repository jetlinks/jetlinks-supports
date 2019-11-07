package org.jetlinks.supports.official.types;

import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.DataTypeCodec;

import java.util.HashMap;
import java.util.Map;

import static java.util.Optional.ofNullable;

public abstract class AbstractDataTypeCodec<T extends DataType> implements DataTypeCodec<T> {

    @Override
    public T decode(T type, Map<String, Object> config) {
        ofNullable(config.get("description"))
                .map(String::valueOf)
                .ifPresent(type::setDescription);

        ofNullable(config.get("expands"))
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .ifPresent(type::setExpands);
        return type;
    }

    @Override
    public Map<String, Object> encode(T type) {
        Map<String, Object> encoded = new HashMap<>();
        encoded.put("type", getTypeId());
        encoded.put("description", type.getDescription());
        encoded.put("expands", type.getExpands());
        doEncode(encoded, type);
        return encoded;
    }

    protected void doEncode(Map<String, Object> encoded, T type) {

    }
}
