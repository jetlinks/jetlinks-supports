package org.jetlinks.supports.official;

import io.swagger.v3.oas.annotations.media.Schema;
import org.hswebframework.web.dict.EnumDict;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.types.*;
import org.springframework.core.ResolvableType;
import org.springframework.util.ReflectionUtils;
import reactor.util.function.Tuples;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;

public class DeviceMetadataParser {

    private DeviceMetadataParser() {

    }

    Set<Object> distinct = new HashSet<>();

    public static PropertyMetadata withField(Field field, ResolvableType type) {
        return new DeviceMetadataParser().withField0(null, field, type);
    }

    public static DataType withType(ResolvableType type) {
        return new DeviceMetadataParser().withType0(null, type);
    }


    private PropertyMetadata withField0(Object owner, Field field, ResolvableType type) {

        Schema schema = field.getAnnotation(Schema.class);
        String id = field.getName();
        String name = schema == null ? field.getName() : schema.description();

        JetLinksPropertyMetadata metadata = new JetLinksPropertyMetadata();
        metadata.setId(id);
        metadata.setName(name);
        metadata.setDataType(withType0(field, type));

        return metadata;

    }

    private DataType withType0(Object owner, ResolvableType type) {
        Class<?> clazz = type.toClass();
        if (clazz == Object.class) {
            return null;
        }
        if (List.class.isAssignableFrom(clazz)) {
            ArrayType arrayType = new ArrayType();
            arrayType.setElementType(withType0(owner, type.getGeneric(0)));
            return arrayType;
        }
        if (clazz.isArray()) {
            ArrayType arrayType = new ArrayType();
            arrayType.setElementType(withType0(owner, ResolvableType.forType(clazz.getComponentType())));
            return arrayType;
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return new ObjectType();
        }
        if (clazz == String.class || clazz == Character.class) {
            return new StringType();
        }
        if (clazz == byte.class || clazz == Byte.class) {
            return new IntType().max(Byte.MAX_VALUE);
        }
        if (clazz == short.class || clazz == Short.class) {
            return new IntType().max(Short.MAX_VALUE).min(0);
        }
        if (clazz == int.class || clazz == Integer.class) {
            return new IntType();
        }
        if (clazz == long.class || clazz == Long.class) {
            return new LongType();
        }
        if (clazz == float.class || clazz == Float.class) {
            return new FloatType();
        }
        if (clazz == double.class || clazz == Double.class) {
            return new DoubleType();
        }
        if (clazz == Date.class || clazz == LocalDateTime.class) {
            return new DateTimeType();
        }
        if (clazz == Boolean.class || clazz == boolean.class) {
            return new BooleanType();
        }
        if (clazz.isEnum()) {
            EnumType enumType = new EnumType();
            for (Object constant : clazz.getEnumConstants()) {
                if (constant instanceof EnumDict) {
                    EnumDict<?> dict = ((EnumDict<?>) constant);
                    enumType.addElement(EnumType.Element.of(String.valueOf(dict.getValue()), dict.getText()));
                } else {
                    Enum<?> dict = ((Enum<?>) constant);
                    enumType.addElement(EnumType.Element.of(dict.name(), dict.name()));
                }
            }
            return enumType;
        }

        ObjectType objectType = new ObjectType();

        ReflectionUtils.doWithFields(type.toClass(), field -> {
            if (owner != null && !distinct.add(Tuples.of(owner, field))) {
                objectType.addPropertyMetadata(withField0(type.toClass(), field, ResolvableType.forClass(Map.class)));
                return;
            }
            Schema schema = field.getAnnotation(Schema.class);
            if (schema != null && !schema.hidden()) {
                objectType.addPropertyMetadata(withField0(type.toClass(), field, ResolvableType.forField(field, type)));
            }
        });
        return objectType;
    }

}
