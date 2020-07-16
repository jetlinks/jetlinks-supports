package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryDecoder;

@AllArgsConstructor(staticName = "of")
public class AppendBinaryDecoder implements BinaryDecoder<String> {

    private final BinaryDecoder<?>[] decoders;

    @Override
    public String decode(byte[] payload, int offset) {

        StringBuilder builder = new StringBuilder();

        for (BinaryDecoder<?> decoder : decoders) {
            builder.append(decoder.decode(payload, offset));
        }

        return builder.toString();
    }
}
