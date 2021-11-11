package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.MergeOption;
import org.jetlinks.core.things.ThingMetadata;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class JetLinksDeviceMetadata extends DefaultThingsMetadata implements DeviceMetadata {

    public JetLinksDeviceMetadata(String id, String name) {
        super(id,name);
    }

    public JetLinksDeviceMetadata(JSONObject jsonObject) {
       super(jsonObject);
    }

    public JetLinksDeviceMetadata(DeviceMetadata another) {
       super(another);
    }

    @Override
    protected DefaultThingsMetadata copy() {
        return new JetLinksDeviceMetadata(this);
    }

    @Override
    public <T extends ThingMetadata> DeviceMetadata merge(T metadata, MergeOption... options) {
        return (DeviceMetadata)super.merge(metadata, options);
    }
}
