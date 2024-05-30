package org.jetlinks.supports.official;

import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.utils.MetadataUtils;
import org.springframework.core.ResolvableType;

import java.lang.reflect.Field;

public class DeviceMetadataParser {

    private DeviceMetadataParser() {

    }

    public static PropertyMetadata withField(Field field, ResolvableType type) {
        return MetadataUtils.parseProperty(field, type);
    }

    public static DataType withType(ResolvableType type) {
        return MetadataUtils.parseType(type);
    }


}
