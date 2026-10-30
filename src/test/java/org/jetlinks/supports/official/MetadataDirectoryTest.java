package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.DeviceMetadataCodecs;
import org.jetlinks.core.metadata.FunctionKind;
import org.jetlinks.core.metadata.MetadataSemantics;
import org.jetlinks.core.metadata.PropertySemanticRole;
import org.jetlinks.core.things.ThingMetadata;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/** 默认设备及通用 Codec 的完整目录往返，不依赖平台或业务实现。 */
public class MetadataDirectoryTest {

    @Test
    public void should_roundtrip_full_metadata_through_default_device_and_thing_codecs() throws Exception {
        String source = source();
        var codec = DeviceMetadataCodecs.defaultCodec();
        DeviceMetadata device = codec.decode(source).block();
        assertModel(device);
        assertModel(codec.decode(codec.encode(device).block()).block());

        var things = DefaultThingMetadataCodec.getInstance();
        ThingMetadata model = things.decode(source).block();
        assertModel(model);
        assertModel(things.decode(things.encode(model).block()).block());
    }

    @Test
    public void should_preserve_directories_when_copying_merging_and_reloading_models() throws Exception {
        DefaultThingsMetadata model = new DefaultThingsMetadata(JSON.parseObject(source()));
        assertModel(new DefaultThingsMetadata(model));
        JSONObject additional = JSON.parseObject("{\"id\":\"other\",\"expands\":{\"newKey\":\"newValue\"},"
            + "\"relations\":{\"device\":[{\"id\":\"related\",\"reverse\":true,"
            + "\"expands\":{\"semantic\":\"urn:example:future\"}}]},\"modules\":[{\"id\":\"metrics\"}]}");
        ThingMetadata merged = model.merge(new DefaultThingsMetadata(additional));
        assertModel(merged);
        assertEquals("newValue", merged.getExpands().get("newKey"));
        assertEquals(2, merged.getRelations().size());
        assertTrue(merged.getRelations("device").get(0).isReverse());
        assertEquals(2, merged.getModules().size());

        // 已读取的目录和属性缓存不能污染后续 fromJson 的模型身份与定义。
        model.fromJson(additional);
        assertEquals("other", model.getId());
        assertTrue(model.getProperties().isEmpty());
        assertTrue(model.getFunctions().isEmpty());
        assertTrue(model.getRelations("space").isEmpty());
        assertEquals("metrics", model.getModules().get(0).getId());
        assertEquals("urn:example:future", model.getRelations("device").get(0).getExpands().get("semantic"));
    }

    @Test
    public void should_keep_optional_directories_absent_in_legacy_json() {
        DefaultThingsMetadata model = new DefaultThingsMetadata(JSON.parseObject("{\"id\":\"legacy\",\"name\":\"Legacy\"}"));
        JSONObject encoded = model.toJson();
        assertEquals("legacy", encoded.getString("id"));
        assertFalse(encoded.containsKey("relations"));
        assertFalse(encoded.containsKey("modules"));
        assertTrue(model.getRelations().isEmpty());
        assertTrue(model.getModules().isEmpty());
    }

    private static void assertModel(ThingMetadata model) {
        assertNotNull(model);
        assertEquals("pump-template", model.getId());
        assertEquals("Pump", model.getName());
        assertEquals("Pump metadata with telemetry module", model.getDescription());
        assertTrue(model.toJson().getJSONObject("expands").getJSONArray("semanticTypes").contains("urn:example:pump"));
        assertEquals(2, model.toJson().getJSONObject("expands").getJSONObject("future").getIntValue("revision"));
        assertEquals(PropertySemanticRole.result, MetadataSemantics.propertyRole(model.getPropertyOrNull("temperature")).orElseThrow());
        assertEquals("double", model.getPropertyOrNull("temperature").getValueType().getId());
        assertEquals("celsius", model.getPropertyOrNull("temperature").toJson().getJSONObject("valueType").getString("unit"));
        assertEquals(FunctionKind.action, MetadataSemantics.functionKind(model.getFunctionOrNull("stop")).orElseThrow());
        assertEquals("string", model.getFunctionOrNull("stop").getInputs().get(0).getValueType().getId());
        assertEquals("boolean", model.getFunctionOrNull("stop").getOutput().getId());
        assertNotNull(model.getEventOrNull("overheat"));
        assertNotNull(model.getTagOrNull("batch"));
        assertEquals("urn:example:location", model.getRelations("space").get(0).getExpands().get("future"));
        ThingMetadata telemetry = model.getModule("telemetry").orElseThrow();
        assertEquals(PropertySemanticRole.observedAt,
            MetadataSemantics.propertyRole(telemetry.getPropertyOrNull("observedAt")).orElseThrow());
        assertEquals("about", telemetry.getRelations("device").get(0).getExpands().get("semantic"));
    }

    private static String source() throws Exception {
        try (InputStream input = new ClassPathResource("metadata-directory.json").getInputStream()) {
            return StreamUtils.copyToString(input, StandardCharsets.UTF_8);
        }
    }
}
