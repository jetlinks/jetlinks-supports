package org.jetlinks.supports.protocol.codec;

import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.MessageEncodeContext;

public interface BlockingEncoder {

    EncodedMessage encode(MessageEncodeContext context);

}
