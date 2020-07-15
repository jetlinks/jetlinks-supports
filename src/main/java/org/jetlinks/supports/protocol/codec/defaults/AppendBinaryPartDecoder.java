package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryPartDecoder;

@AllArgsConstructor(staticName = "of")
public class AppendBinaryPartDecoder implements BinaryPartDecoder<String> {

    private final BinaryPartDecoder<?>[] decoders;

    @Override
    public String decode(byte[] payload, int offset) {

        StringBuilder builder = new StringBuilder();

        for (BinaryPartDecoder<?> decoder : decoders) {
            builder.append(decoder.decode(payload, offset));
        }

        return builder.toString();
    }
}
