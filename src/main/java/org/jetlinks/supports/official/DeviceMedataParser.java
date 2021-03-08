package org.jetlinks.supports.official;

import io.swagger.v3.oas.annotations.media.Schema;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.types.*;
import org.springframework.core.ResolvableType;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

public class DeviceMedataParser {


    public static PropertyMetadata withField(Field field) {
        Schema schema = field.getAnnotation(Schema.class);
        String id = field.getName();
        String name = schema == null ? field.getName() : schema.description();

        JetLinksPropertyMetadata metadata = new JetLinksPropertyMetadata();
        metadata.setId(id);
        metadata.setName(name);
        metadata.setDataType(withType(ResolvableType.forField(field)));

        return metadata;

    }

    public static DataType withType(ResolvableType type) {
        Class<?> clazz = type.toClass();
        if (clazz.isAssignableFrom(List.class)) {
            ArrayType arrayType = new ArrayType();
            arrayType.setElementType(withType(type.getGeneric(0)));
            return arrayType;
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

        ObjectType objectType = new ObjectType();

        ReflectionUtils.doWithFields(type.toClass(), field -> {
            Schema schema = field.getAnnotation(Schema.class);
            if (schema != null && !schema.hidden()) {
                objectType.addPropertyMetadata(withField(field));
            }
        });
        return objectType;
    }

}
