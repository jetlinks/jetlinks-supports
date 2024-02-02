package org.jetlinks.supports.utils;

import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.SimplePropertyMetadata;
import org.jetlinks.supports.official.DeviceMetadataParser;
import org.springframework.core.ResolvableType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeviceMetadataUtils {


    public static List<PropertyMetadata> convertToProperties(Map<String, Object> value) {
        return value
            .entrySet()
            .stream()
            .filter(e -> e.getValue() != null)
            .map(entry -> {
                SimplePropertyMetadata metadata = new SimplePropertyMetadata();
                metadata.setId(entry.getKey());
                metadata.setName(entry.getKey());
                metadata.setValueType(DeviceMetadataParser.withType(
                    ResolvableType.forType(entry.getValue().getClass())
                ));
                return metadata;
            })
            .collect(Collectors.toList());
    }

}
