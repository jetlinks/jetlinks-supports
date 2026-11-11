package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.DeviceMetadataCodecs;
import org.jetlinks.core.metadata.FunctionKind;
import org.jetlinks.core.metadata.MetadataSemantics;
import org.jetlinks.core.metadata.PropertySemanticRole;
import org.jetlinks.core.metadata.RelationMetadata;
import org.jetlinks.core.metadata.SimpleDeviceMetadata;
import org.hswebframework.web.exception.I18nSupportException;
import org.jetlinks.core.things.ThingMetadata;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/** 默认设备及通用 Codec 的完整目录往返，不依赖平台或业务实现。 */
public class MetadataDirectoryTest {

    @Test
    public void should_decode_and_encode_empty_root_metadata() {
        for (String input : new String[]{null, "", "   ", "null", "{}"}) {
            DeviceMetadata device = JetLinksDeviceMetadataCodec.getInstance().doDecode(input);
            ThingMetadata thing = DefaultThingMetadataCodec.getInstance().doDecode(input);
            assertEquals(new SimpleDeviceMetadata().toJson(), device.toJson());
            assertEquals(device.toJson(), thing.toJson());
        }
    }

    @Test
    public void should_clear_identity_and_warm_directories_when_reloading_null() throws Exception {
        DefaultThingsMetadata model = new DefaultThingsMetadata(JSON.parseObject(source()));
        assertModel(model);
        model.fromJson(null);
        assertNull(model.getId());
        assertNull(model.getName());
        assertNull(model.getDescription());
        assertNull(model.getExpands());
        assertEquals(new SimpleDeviceMetadata().toJson(), model.toJson());
    }

    @Test
    public void should_reject_non_object_directory_entries() {
        for (String directory : new String[]{"properties", "functions", "events", "tags", "relations", "modules"}) {
            for (String entry : new String[]{"null", "7", "true", "\"invalid\"", "[]"}) {
                DefaultThingsMetadata model = new DefaultThingsMetadata(
                    JSON.parseObject("{\"" + directory + "\":[" + entry + "]}"));
                assertInvalidDefinition(model::toJson);
            }
        }
        DefaultThingsMetadata nested = new DefaultThingsMetadata(
            JSON.parseObject("{\"modules\":[{\"id\":\"nested\",\"relations\":[null]}]}"));
        assertInvalidDefinition(nested::toJson);
        DefaultThingsMetadata object = new DefaultThingsMetadata(JSON.parseObject(
            "{\"properties\":[{\"id\":\"object\",\"valueType\":{\"type\":\"object\",\"properties\":[null]}}]}"));
        assertInvalidDefinition(object::toJson);
    }

    @Test
    public void should_isolate_copied_and_merged_relation_and_module_objects() throws Exception {
        for (boolean merge : new boolean[]{false, true}) {
            DefaultThingsMetadata original = new DefaultThingsMetadata(JSON.parseObject(source()));
            ThingMetadata copied = merge
                ? original.merge(new DefaultThingsMetadata("additional", "Additional"))
                : new DefaultThingsMetadata(original);
            String originalRelationName = original.getRelationMetadata().get(0).getName();
            String originalModuleName = original.getModules().get(0).getName();
            copied.getRelationMetadata().get(0).setName("copy-relation");
            ((DefaultThingsMetadata) copied.getModules().get(0)).setName("copy-module");
            copied.getRelationMetadata().get(0).getExpands().put("copy-only", true);
            assertEquals(originalRelationName, original.getRelationMetadata().get(0).getName());
            assertEquals(originalModuleName, original.getModules().get(0).getName());
            assertFalse(original.getRelationMetadata().get(0).getExpands().containsKey("copy-only"));
            original.getRelationMetadata().get(0).setName("source-relation");
            ((DefaultThingsMetadata) original.getModules().get(0)).setName("source-module");
            assertEquals("copy-relation", copied.getRelationMetadata().get(0).getName());
            assertEquals("copy-module", copied.getModules().get(0).getName());
        }
    }

    @Test
    public void should_snapshot_mutable_source_directory_collections() {
        RelationMetadata relation = new RelationMetadata();
        relation.setId("about");
        relation.setTargetType("device");
        SimpleDeviceMetadata original = new SimpleDeviceMetadata();
        original.setRelationMetadata(new ArrayList<>(List.of(relation)));
        original.setModules(new ArrayList<>(List.of(new DefaultThingsMetadata("nested", "Nested"))));
        DefaultThingsMetadata copied = new DefaultThingsMetadata(original);
        original.getRelationMetadata().clear();
        original.getModules().clear();
        assertEquals(1, copied.getRelationMetadata().size());
        assertEquals("nested", copied.getModules().get(0).getId());
    }

    @Test
    public void should_roundtrip_simple_models_through_default_codecs() throws Exception {
        SimpleDeviceMetadata simple = new SimpleDeviceMetadata();
        simple.fromJson(JSON.parseObject(source()));
        assertModel(simple);
        var codec = DeviceMetadataCodecs.defaultCodec();
        assertModel(codec.decode(codec.encode(simple).block()).block());
        assertModel(new DefaultThingsMetadata(simple));
    }

    @Test
    public void should_append_typed_relations_without_losing_lazy_directory_or_aliasing_collection() throws Exception {
        DefaultThingsMetadata model = new DefaultThingsMetadata(JSON.parseObject(source()));
        RelationMetadata relation = new RelationMetadata();
        relation.setId("parent");
        relation.setTargetType("org");
        List<RelationMetadata> additions = new ArrayList<>(List.of(relation));
        model.addRelations(additions);
        additions.clear();
        assertEquals(2, model.getRelationMetadata().size());
        assertEquals(1, model.getRelationMetadata("space").size());
        ThingMetadata decoded = new DefaultThingsMetadata(model.toJson());
        assertEquals("parent", decoded.getRelationMetadata("org").get(0).getId());
        assertEquals(model.getRelationMetadata().size(), decoded.getRelationMetadata().size());
    }

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
            + "\"relations\":[{\"targetType\":\"device\",\"id\":\"related\",\"reverse\":true,"
            + "\"expands\":{\"semantic\":\"urn:example:future\"}}],\"modules\":[{\"id\":\"metrics\"}]}");
        ThingMetadata merged = model.merge(new DefaultThingsMetadata(additional));
        assertModel(merged);
        assertEquals("newValue", merged.getExpands().get("newKey"));
        assertEquals(2, merged.getRelationMetadata().size());
        assertTrue(merged.getRelationMetadata("device").get(0).isReverse());
        assertEquals(2, merged.getModules().size());

        // 已读取的目录和属性缓存不能污染后续 fromJson 的模型身份与定义。
        model.fromJson(additional);
        assertEquals("other", model.getId());
        assertTrue(model.getProperties().isEmpty());
        assertTrue(model.getFunctions().isEmpty());
        assertTrue(model.getRelationMetadata("space").isEmpty());
        assertEquals("metrics", model.getModules().get(0).getId());
        assertEquals("urn:example:future", model.getRelationMetadata("device").get(0).getExpands().get("semantic"));
    }

    @Test
    public void should_keep_optional_directories_absent_in_legacy_json() {
        DefaultThingsMetadata model = new DefaultThingsMetadata(JSON.parseObject("{\"id\":\"legacy\",\"name\":\"Legacy\"}"));
        JSONObject encoded = model.toJson();
        assertEquals("legacy", encoded.getString("id"));
        assertFalse(encoded.containsKey("relations"));
        assertFalse(encoded.containsKey("modules"));
        assertTrue(model.getRelationMetadata().isEmpty());
        assertTrue(model.getModules().isEmpty());
    }

    private static void assertInvalidDefinition(Runnable action) {
        StepVerifier.create(Mono.fromRunnable(action))
                    .expectError(I18nSupportException.class)
                    .verify();
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
        assertEquals("urn:example:location", model.getRelationMetadata("space").get(0).getExpands().get("future"));
        ThingMetadata telemetry = model.getModule("telemetry").orElseThrow();
        assertEquals(PropertySemanticRole.observedAt,
            MetadataSemantics.propertyRole(telemetry.getPropertyOrNull("observedAt")).orElseThrow());
        assertEquals("about", telemetry.getRelationMetadata("device").get(0).getExpands().get("semantic"));
    }

    private static String source() throws Exception {
        try (InputStream input = new ClassPathResource("metadata-directory.json").getInputStream()) {
            return StreamUtils.copyToString(input, StandardCharsets.UTF_8);
        }
    }
}
