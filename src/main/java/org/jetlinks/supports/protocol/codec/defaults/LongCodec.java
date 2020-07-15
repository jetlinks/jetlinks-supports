package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryCodec;
import org.jetlinks.supports.protocol.codec.Endian;

@AllArgsConstructor(staticName = "of")
public class LongCodec implements BinaryCodec<Long> {

    private final Endian endian;
    private final int offset;
    private final int length;

    @Override
    public Long decode(byte[] payload, int offset) {
        return endian.decode(payload, this.offset + offset, length);
    }

    @Override
    public void encode(Long part, byte[] payload, int offset) {
        endian.encode(part, payload, this.offset + offset, length);
    }

}
