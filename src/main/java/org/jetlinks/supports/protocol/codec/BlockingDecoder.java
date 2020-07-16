package org.jetlinks.supports.protocol.codec;

import org.jetlinks.core.message.DeviceMessage;

/**
 *
 * @see BlockingDecoderBuilder
 */
public interface BlockingDecoder {

    static BlockingDecoderBuilder.BlockingDecoderDeclaration declare() {
        return new DefaultBlockingDecoderBuilder().declare();
    }

    DeviceMessage decode(byte[] message,int offset);

}
