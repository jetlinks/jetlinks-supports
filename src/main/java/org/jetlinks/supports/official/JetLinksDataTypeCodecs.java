package org.jetlinks.supports.official;

import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.DataTypeCodec;
import org.jetlinks.supports.official.types.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JetLinksDataTypeCodecs {

    private static final Map<String, DataTypeCodec<? extends DataType>> codecMap = new HashMap<>();

    static {
        register(new JetLinksArrayCodec());
        register(new JetLinksBooleanCodec());
        register(new JetLinksDateCodec());
        register(new JetLinksDoubleCodec());
        register(new JetLinksEnumCodec());
        register(new JetLinksFloatCodec());
        register(new JetLinksGeoPointCodec());
        register(new JetLinksIntCodec());
        register(new JetLinksLongCodec());
        register(new JetLinksObjectCodec());
        register(new JetLinksStringCodec());
        register(new JetLinksPasswordCodec());
        register(new JetLinksFileCodec());
        register(new JetLinksGeoShapeCodec());
    }

    public static void register(DataTypeCodec<? extends DataType> codec) {
        codecMap.put(codec.getTypeId(), codec);
    }

    @SuppressWarnings("all")
    public static Optional<DataTypeCodec<DataType>> getCodec(String typeId) {

        return Optional.ofNullable((DataTypeCodec) codecMap.get(typeId));
    }

    public static DataType decode(DataType type, Map<String, Object> config) {
        if (type == null) {
            return null;
        }
        return getCodec(type.getId())
                .map(codec -> codec.decode(type, config))
                .orElse(type);
    }

    public static Optional<Map<String, Object>> encode(DataType type) {
        if (type == null) {
            return Optional.empty();
        }
        return getCodec(type.getId())
                .map(codec -> codec.encode(type));
    }
}
