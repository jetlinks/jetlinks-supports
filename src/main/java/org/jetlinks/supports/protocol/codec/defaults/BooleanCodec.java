package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.*;

@AllArgsConstructor(staticName = "of")
public class BooleanCodec implements BinaryCodec<Boolean> {

    private final int offset;

    @Override
    public Boolean decode(byte[] payload, int offset) {
        return payload[offset + this.offset] != 0;
    }

    @Override
    public void encode(Boolean part, byte[] payload, int offset) {
        payload[offset + this.offset] = Boolean.TRUE.equals(part) ? (byte) 0x01 : (byte) 0x00;
    }
}
