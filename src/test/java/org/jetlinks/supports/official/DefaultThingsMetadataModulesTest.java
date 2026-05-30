package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.jetlinks.core.things.ThingMetadata;
import org.junit.Assert;
import org.junit.Test;

public class DefaultThingsMetadataModulesTest {

    @Test
    public void testDecodeAndEncodeModules() {
        JSONObject json = JSON.parseObject("{\n" +
            "  \"id\": \"device\",\n" +
            "  \"name\": \"设备\",\n" +
            "  \"properties\": [],\n" +
            "  \"functions\": [],\n" +
            "  \"events\": [],\n" +
            "  \"tags\": [],\n" +
            "  \"modules\": [{\n" +
            "    \"id\": \"temperature-board\",\n" +
            "    \"name\": \"温控板\",\n" +
            "    \"properties\": [],\n" +
            "    \"functions\": [],\n" +
            "    \"events\": [],\n" +
            "    \"tags\": []\n" +
            "  }]\n" +
            "}");

        DefaultThingsMetadata metadata = new DefaultThingsMetadata(json);

        Assert.assertEquals(1, metadata.getModules().size());
        ThingMetadata module = metadata.getModule("temperature-board").orElse(null);
        Assert.assertNotNull(module);
        Assert.assertEquals("温控板", module.getName());

        JSONObject encoded = metadata.toJson();
        Assert.assertTrue(encoded.containsKey("modules"));
        Assert.assertEquals(1, encoded.getJSONArray("modules").size());
        Assert.assertEquals("temperature-board", encoded.getJSONArray("modules").getJSONObject(0).getString("id"));
    }

    @Test
    public void testCopyKeepsModules() {
        DefaultThingsMetadata metadata = new DefaultThingsMetadata("device", "设备");
        metadata.addModule(new DefaultThingsMetadata("module-a", "模块A"));

        DefaultThingsMetadata copied = new DefaultThingsMetadata(metadata);

        Assert.assertEquals(1, copied.getModules().size());
        Assert.assertNotNull(copied.getModuleOrNull("module-a"));
        Assert.assertEquals(1, copied.toJson().getJSONArray("modules").size());
    }
}
