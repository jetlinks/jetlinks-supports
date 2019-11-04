package org.jetlinks.supports.official;

import lombok.SneakyThrows;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.EventMetadata;
import org.jetlinks.core.metadata.FunctionMetadata;
import org.jetlinks.core.metadata.types.BooleanType;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class JetLinksDeviceMetadataCodecTest {

    @Test
    @SneakyThrows
    public void test() {
        String metadata = StreamUtils.copyToString(new ClassPathResource("jetlinks.metadata.json").getInputStream(), StandardCharsets.UTF_8);

        JetLinksDeviceMetadataCodec metadataCodec = new JetLinksDeviceMetadataCodec();
        DeviceMetadata deviceMetadata = metadataCodec.decode(metadata).block();

        Assert.assertTrue(deviceMetadata.getEvent("fire_alarm").isPresent());

        EventMetadata eventMetadata = deviceMetadata.getEvent("fire_alarm").get();

        assertEquals("object", eventMetadata.getType().getId());

        Assert.assertTrue(deviceMetadata.getFunction("playVoice").isPresent());

        DataType type= deviceMetadata.getFunction("playVoice").map(FunctionMetadata::getOutput).get();

        Assert.assertTrue(type instanceof BooleanType);

    }
}