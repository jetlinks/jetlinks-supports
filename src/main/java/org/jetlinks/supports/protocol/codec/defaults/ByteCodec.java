package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.*;

@AllArgsConstructor(staticName = "of")
public class ByteCodec implements BinaryCodec<Byte> {

    private final int offset;

    @Override
    public Byte decode(byte[] payload, int offset) {
        return payload[this.offset + offset];
    }

    @Override
    public void encode(Byte part, byte[] payload, int offset) {
        payload[this.offset + offset] = part;
    }
}
