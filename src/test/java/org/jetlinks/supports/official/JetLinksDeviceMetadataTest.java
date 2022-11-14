package org.jetlinks.supports.official;

import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.types.StringType;
import org.junit.Test;

import static org.junit.Assert.*;

public class JetLinksDeviceMetadataTest {

    @Test
    public void testProperties(){
        JetLinksDeviceMetadata metadata = new JetLinksDeviceMetadata("test","test");

        metadata.addProperty(new JetLinksPropertyMetadata("test", "test", StringType.GLOBAL));
        metadata.addProperty(new JetLinksPropertyMetadata("test2", "test2", StringType.GLOBAL));


        assertEquals(2,metadata.getProperties().size());

        {
            JetLinksDeviceMetadata newMetadata = new JetLinksDeviceMetadata(metadata);
            assertEquals(2,newMetadata.getProperties().size());
        }

        {
            DeviceMetadata newMetadata = new JetLinksDeviceMetadata("test", "test").merge(metadata);

            assertEquals(2,newMetadata.getProperties().size());
        }

    }
}