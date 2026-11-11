package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import org.hswebframework.web.exception.I18nSupportException;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.SimpleDeviceMetadata;
import org.jetlinks.core.metadata.SimpleEventMetadata;
import org.jetlinks.core.metadata.types.ArrayType;
import org.jetlinks.core.metadata.types.EnumType;
import org.jetlinks.core.utils.json.ObjectMappers;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/** 覆盖可选类型、未注册嵌套类型及字符串输出的真实元数据入口。 */
public class MetadataNpeRegressionTest {

    @Test
    public void should_roundtrip_a_simple_event_without_type_through_default_codec() {
        SimpleDeviceMetadata simple = new SimpleDeviceMetadata();
        simple.fromJson(json("{\"events\":[{\"id\":\"e\"}]}"));
        JetLinksDeviceMetadataCodec codec = JetLinksDeviceMetadataCodec.getInstance();
        var decoded = codec.doDecode(codec.doEncode(simple));
        assertEquals("unknown", decoded.getEventOrNull("e").getType().getId());
        assertEquals(decoded.toJson(), codec.doDecode(codec.doEncode(decoded)).toJson());
    }

    @Test
    public void should_copy_an_event_whose_source_returns_a_null_type() {
        SimpleEventMetadata source = new SimpleEventMetadata() {
            @Override
            public DataType getType() {
                return null;
            }
        };
        source.setId("legacy");
        JetLinksEventMetadata copied = new JetLinksEventMetadata(source);
        assertEquals("unknown", copied.getType().getId());
        JetLinksEventMetadata decoded = new JetLinksEventMetadata(copied.toJson());
        assertEquals("unknown", decoded.getType().getId());
        assertEquals(copied.toJson(), decoded.toJson());
    }

    @Test
    public void should_use_unknown_for_missing_null_empty_or_unregistered_event_types() {
        for (String event : List.of("{\"id\":\"e\"}", "{\"id\":\"e\",\"valueType\":null}",
            "{\"id\":\"e\",\"valueType\":{}}", "{\"id\":\"e\",\"valueType\":{\"type\":\"unregistered\"}}")) {
            DefaultThingsMetadata model = new DefaultThingsMetadata(json("{\"events\":[" + event + "]}"));
            DataType type = model.getEventOrNull("e").getType();
            assertEquals("unknown", type.getId());
            assertEquals("raw", type.format("raw"));
            assertEquals(model.toJson(), new DefaultThingsMetadata(model.toJson()).toJson());
        }
    }

    @Test
    public void should_reject_explicit_missing_or_unregistered_nested_types_with_format_error() {
        for (String kind : List.of("array", "enum")) {
            for (Object invalid : List.of(Map.of(), Map.of("type", "unregistered"), "unregistered", json("{\"type\":null}"))) {
                DefaultThingsMetadata model = model(kind, Map.of(nestedKey(kind), invalid));
                StepVerifier.create(Mono.fromRunnable(model::toJson))
                            .expectError(I18nSupportException.class)
                            .verify();
            }
        }
    }

    @Test
    public void should_preserve_supported_nested_type_names_and_objects() {
        for (String kind : List.of("array", "enum")) {
            for (Object definition : List.of("string", Map.of("type", "string"))) {
                DefaultThingsMetadata model = model(kind, Map.of(nestedKey(kind), definition));
                assertEquals("string", nestedType(model.getPropertyOrNull("p").getValueType()).getId());
                DefaultThingsMetadata decoded = new DefaultThingsMetadata(model.toJson());
                assertEquals("string", nestedType(decoded.getPropertyOrNull("p").getValueType()).getId());
            }
        }
    }

    @Test
    public void should_keep_missing_or_null_nested_types_optional() {
        for (String kind : List.of("array", "enum")) {
            DefaultThingsMetadata missing = model(kind, Map.of());
            DefaultThingsMetadata explicitNull = model(kind, json("{\"" + nestedKey(kind) + "\":null}"));
            for (DefaultThingsMetadata model : List.of(missing, explicitNull)) {
                assertNull(nestedType(model.getPropertyOrNull("p").getValueType()));
                assertEquals(model.toJson(), new DefaultThingsMetadata(model.toJson()).toJson());
            }
        }
    }

    @Test
    public void should_format_untyped_properties_without_changing_their_optional_type() {
        for (JetLinksPropertyMetadata property : List.of(new JetLinksPropertyMetadata(),
            new JetLinksPropertyMetadata(json("{\"id\":\"p\",\"name\":\"P\"}")),
            new JetLinksPropertyMetadata(json("{\"id\":\"p\",\"valueType\":null}")))) {
            assertNull(property.getValueType());
            assertTrue(property.toString().startsWith("unknown "));
            assertNull(property.getValueType());
            assertNull(property.toJson().get("valueType"));
        }
        JetLinksPropertyMetadata typed = new JetLinksPropertyMetadata(json(
            "{\"id\":\"p\",\"name\":\"P\",\"valueType\":{\"type\":\"string\"}}"));
        assertEquals("string p /* P */ ", typed.toString());
    }

    private static DefaultThingsMetadata model(String kind, Map<String, Object> fields) {
        JSONObject type = new JSONObject();
        type.put("type", kind);
        type.putAll(fields);
        // 枚举编码在存在 elements 时才输出其余选项，使用真实的非空目录覆盖完整往返。
        if ("enum".equals(kind)) {
            type.put("elements", List.of(Map.of("value", "a", "text", "A", "description", "Description")));
        }
        return new DefaultThingsMetadata(new JSONObject(Map.of("properties", List.of(Map.of("id", "p", "valueType", type)))));
    }

    private static String nestedKey(String kind) {
        return "array".equals(kind) ? "elementType" : "valueType";
    }

    private static DataType nestedType(DataType type) {
        return type instanceof ArrayType ? ((ArrayType) type).getElementType() : ((EnumType) type).getValueType();
    }

    private static JSONObject json(String definition) {
        return ObjectMappers.parseJson(definition, JSONObject.class);
    }
}
