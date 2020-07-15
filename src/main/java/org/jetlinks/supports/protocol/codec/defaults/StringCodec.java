package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryCodec;

import java.nio.charset.Charset;

@AllArgsConstructor(staticName = "of")
public class StringCodec implements BinaryCodec<String> {

    private final Charset charset;

    private final int offset;
    private final int length;

    @Override
    public String decode(byte[] payload, int offset) {
        offset = this.offset + offset;
        int readableLength = payload.length - offset;
        if (length != -1) {
            readableLength = Math.min(readableLength, length);
        }
        return new String(payload, offset, readableLength);
    }

    @Override
    public void encode(String part, byte[] payload, int offset) {
        byte[] data = part.getBytes(charset);

        System.arraycopy(data, 0, payload, this.offset + offset, data.length);
    }
}
