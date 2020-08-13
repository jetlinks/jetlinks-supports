package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryCodec;
import org.jetlinks.supports.protocol.codec.Endian;

/**
 * 2字节转float
 * <p>
 * 算法: 第一个字节为整数位，第二字节为小数位
 * <p>
 * e.g.
 * <pre>
 *     [02,22] = 2,34 = 2.34
 * </pre>
 *
 * @since 1.1.1
 */
@AllArgsConstructor(staticName = "of")
public class TwoBytesFloatCodec implements BinaryCodec<Float> {

    private final Endian endian;

    private final int offset;

    @Override
    public Float decode(byte[] payload, int offset) {
        int high = payload[offset + this.offset] ;
        int low = payload[offset + this.offset + 1];
        //默认大端.小端时,低字节在前.
        if (endian == Endian.Little) {
            int tmp = low;
            low = high;
            high = tmp;
        }
        return high + (low == 0 ? 0.0F : (float) (low / Math.pow(10, ((int) Math.log10(low) + 1))));
    }


    @Override
    public void encode(Float part, byte[] payload, int offset) {
        int high = (int) Math.floor(part);
        int low = Math.round(((part - high) * 100));
        if (endian == Endian.Little) {
            payload[offset + this.offset] = (byte) low;
            payload[offset + this.offset + 1] = (byte) high;
        } else {
            payload[offset + this.offset] = (byte) high;
            payload[offset + this.offset + 1] = (byte) low;
        }
    }
}
