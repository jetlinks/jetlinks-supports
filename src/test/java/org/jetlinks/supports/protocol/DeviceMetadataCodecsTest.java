package org.jetlinks.supports.protocol;

import org.jetlinks.core.metadata.DeviceMetadataCodecs;
import org.junit.Assert;
import org.junit.Test;

public class DeviceMetadataCodecsTest {

    @Test
    public void test(){
        Assert.assertNotNull(DeviceMetadataCodecs.defaultCodec());
    }
}
