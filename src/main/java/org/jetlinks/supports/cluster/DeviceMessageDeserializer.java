package org.jetlinks.supports.cluster;

import org.jetlinks.core.message.DeviceMessage;

import java.nio.ByteBuffer;

public interface DeviceMessageDeserializer {
    DeviceMessage deserialize(ByteBuffer byteBuffer);
}
