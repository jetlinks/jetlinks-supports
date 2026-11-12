package org.jetlinks.supports.official;

import org.jetlinks.core.metadata.CompositeDeviceMetadata;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.MergeOption;
import org.jetlinks.core.metadata.SimpleDeviceMetadata;
import org.jetlinks.core.things.ThingMetadata;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/** 覆盖空产品模型、设备独立模型与平台所用组合、合并、复制编码的交叉边界。 */
public class MetadataEmptyModelTest {

    private static final JetLinksDeviceMetadataCodec CODEC = JetLinksDeviceMetadataCodec.getInstance();

    private static final String[] EMPTY_MODELS = {
        null, "", " \t\n ", "null", "{}",
        "{\"properties\":null,\"functions\":null,\"events\":null,\"tags\":null,\"relations\":null,\"modules\":null}",
        "{\"properties\":[],\"functions\":[],\"events\":[],\"tags\":[],\"relations\":[],\"modules\":[]}"
    };

    @Test
    public void should_roundtrip_empty_models_through_reactive_codecs() {
        var things = DefaultThingMetadataCodec.getInstance();
        for (String input : EMPTY_MODELS) {
            DeviceMetadata device = CODEC.decode(input).block();
            assertEmpty(device);
            assertEmpty(CODEC.decode(CODEC.encode(device).block()).block());
            ThingMetadata thing = things.decode(input).block();
            assertEmpty(thing);
            assertEmpty(things.decode(things.encode(thing).block()).block());
        }
        // 产品没有物模型时，平台直接返回 SimpleDeviceMetadata，后续仍使用默认 Codec。
        assertEmpty(CODEC.doDecode(CODEC.doEncode(new SimpleDeviceMetadata())));
    }

    @Test
    public void should_preserve_all_directories_when_either_composite_side_is_empty() throws Exception {
        String source = source();
        for (String input : EMPTY_MODELS) {
            DeviceMetadata populated = CODEC.doDecode(source);
            DeviceMetadata empty = CODEC.doDecode(input);
            for (DeviceMetadata composite : List.of(
                new CompositeDeviceMetadata(empty, populated),
                new CompositeDeviceMetadata(populated, empty),
                new CompositeDeviceMetadata(new SimpleDeviceMetadata(), populated),
                new CompositeDeviceMetadata(populated, new SimpleDeviceMetadata()))) {
                assertDirectories(populated, composite);
                assertDirectories(populated, CODEC.doDecode(CODEC.doEncode(composite)));
                assertNull(composite.getPropertyOrNull("missing"));
                assertNull(composite.getFunctionOrNull("missing"));
                assertNull(composite.getEventOrNull("missing"));
                assertNull(composite.getTagOrNull("missing"));
            }
            assertEmpty(empty);
        }
    }

    @Test
    public void should_preserve_all_directories_when_merging_an_empty_model_on_either_side() throws Exception {
        String source = source();
        for (String input : EMPTY_MODELS) {
            DeviceMetadata populated = CODEC.doDecode(source);
            Map<String, Object> before = populated.toJson();
            DeviceMetadata empty = CODEC.doDecode(input);
            SimpleDeviceMetadata simple = new SimpleDeviceMetadata();
            simple.fromJson(populated.toJson());
            for (DeviceMetadata merged : List.of(
                empty.merge(populated, MergeOption.ignoreExists),
                populated.merge(empty, MergeOption.ignoreExists),
                empty.merge(simple, MergeOption.ignoreExists),
                populated.merge(new SimpleDeviceMetadata(), MergeOption.ignoreExists))) {
                assertDirectories(populated, CODEC.doDecode(CODEC.doEncode(merged)));
            }
            assertEquals(before, populated.toJson());
            assertEmpty(empty);
        }
    }

    @Test
    public void should_keep_both_empty_models_empty_when_combining_or_merging() {
        for (String product : EMPTY_MODELS) {
            for (String device : EMPTY_MODELS) {
                DeviceMetadata left = CODEC.doDecode(product);
                DeviceMetadata right = CODEC.doDecode(device);
                assertEmpty(CODEC.doDecode(CODEC.doEncode(new CompositeDeviceMetadata(left, right))));
                assertEmpty(CODEC.doDecode(CODEC.doEncode(left.merge(right, MergeOption.ignoreExists))));
            }
        }
    }

    @Test
    public void should_retain_existing_definitions_and_append_new_ones_after_an_empty_model() {
        DeviceMetadata existing = CODEC.doDecode("{\"properties\":[{\"id\":\"shared\",\"name\":\"Existing\","
            + "\"valueType\":{\"type\":\"int\"}}]}");
        DeviceMetadata incoming = CODEC.doDecode("{\"properties\":[{\"id\":\"shared\",\"name\":\"Incoming\","
            + "\"valueType\":{\"type\":\"string\"}},{\"id\":\"added\",\"valueType\":{\"type\":\"boolean\"}}]}");
        for (String input : EMPTY_MODELS) {
            DeviceMetadata initialized = CODEC.doDecode(input).merge(existing, MergeOption.ignoreExists);
            for (DeviceMetadata combined : List.of(
                initialized.merge(incoming, MergeOption.ignoreExists),
                new CompositeDeviceMetadata(initialized, incoming))) {
                DeviceMetadata decoded = CODEC.doDecode(CODEC.doEncode(combined));
                assertEquals(2, decoded.getProperties().size());
                assertEquals("Existing", decoded.getPropertyOrNull("shared").getName());
                assertEquals("int", decoded.getPropertyOrNull("shared").getValueType().getId());
                assertEquals("boolean", decoded.getPropertyOrNull("added").getValueType().getId());
            }
        }
    }

    private static void assertEmpty(ThingMetadata model) {
        assertNotNull(model);
        assertTrue(model.getProperties().isEmpty());
        assertTrue(model.getFunctions().isEmpty());
        assertTrue(model.getEvents().isEmpty());
        assertTrue(model.getTags().isEmpty());
        assertTrue(model.getRelationMetadata().isEmpty());
        assertTrue(model.getModules().isEmpty());
        assertNull(model.getPropertyOrNull("missing"));
        assertNull(model.getFunctionOrNull("missing"));
        assertNull(model.getEventOrNull("missing"));
        assertNull(model.getTagOrNull("missing"));
        assertEquals(new SimpleDeviceMetadata().toJson(), model.toJson());
    }

    private static void assertDirectories(ThingMetadata expected, ThingMetadata actual) {
        Map<String, Object> expectedJson = expected.toJson();
        Map<String, Object> actualJson = actual.toJson();
        for (String directory : List.of("properties", "functions", "events", "tags", "relations", "modules")) {
            assertEquals(directory, expectedJson.get(directory), actualJson.get(directory));
        }
    }

    private static String source() throws Exception {
        try (InputStream input = new ClassPathResource("metadata-directory.json").getInputStream()) {
            return StreamUtils.copyToString(input, StandardCharsets.UTF_8);
        }
    }
}
