package org.jetlinks.supports.official;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.types.IntType;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.metadata.types.StringType;
import org.junit.Test;
import org.springframework.core.ResolvableType;

import static org.junit.Assert.*;

public class DeviceMetadataParserTest {


    @Test
    public void testParse() {

        DataType type = DeviceMetadataParser.withType(ResolvableType.forType(TestClazz.class));
        assertTrue(type instanceof ObjectType);

        ObjectType objectType = ((ObjectType) type);

        assertTrue(
                objectType.getProperty("idx").get().getValueType() instanceof IntType
        );

        assertTrue(
                objectType.getProperty("obj").get().getValueType() instanceof ObjectType
        );

        assertTrue(
                objectType.getProperty("id").get().getValueType() instanceof StringType
        );

    }

    @Getter
    @Setter
    static class TestClazz extends Generic<String> {
        @Schema(description = "index")
        private int idx;

        @Schema(description = "obj")
        private Entity obj;

    }

    @Getter
    @Setter
    static class Entity {
        @Schema(description = "name")
        private String name;

    }

    @Getter
    @Setter
    static class Generic<T>{

        @Schema(description = "id")
        private T id;
    }
}