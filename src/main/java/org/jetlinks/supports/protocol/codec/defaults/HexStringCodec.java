package org.jetlinks.supports.protocol.codec.defaults;

import io.netty.buffer.ByteBufUtil;
import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryCodec;

@AllArgsConstructor(staticName = "of")
public class HexStringCodec implements BinaryCodec<String> {

    private final int offset;
    private final int length;

    @Override
    public String decode(byte[] payload, int offset) {
        offset = this.offset + offset;

        int readableLength = payload.length - offset;

        if (length != -1) {
            readableLength = Math.min(length, readableLength);
        }

        return ByteBufUtil.hexDump(payload, this.offset + offset, readableLength);
    }

    @Override
    public void encode(String part, byte[] payload, int offset) {

        byte[] data = ByteBufUtil.decodeHexDump(part);

        System.arraycopy(data, 0, payload, this.offset + offset, data.length);
    }
}
