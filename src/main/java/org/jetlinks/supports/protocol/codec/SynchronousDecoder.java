package org.jetlinks.supports.protocol.codec;

import org.jetlinks.core.message.DeviceMessage;

public interface SynchronousDecoder {

    DeviceMessage decode(byte[] message,int offset);

}
