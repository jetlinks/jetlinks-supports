package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryCodec;
import org.jetlinks.supports.protocol.codec.Endian;

/**
 * 2字节16进制转float
 * <p>
 * 算法: 2字节16进制对应的整数/10
 * <p>
 * e.g.
 * <pre>
 *     [02,22] = 0x222 = 546 , 546/10 = 54.6
 *
 *     [-1,-101] = 0xFF9B = -101 , -101/10 = -10.1
 * </pre>
 *
 * @since 1.1.1
 */
@AllArgsConstructor(staticName = "of")
public class TwoBytesHexFloatCodec implements BinaryCodec<Float> {

    private final Endian endian;

    private final int offset;

    @Override
    public Float decode(byte[] payload, int offset) {
        int high = payload[offset + this.offset];
        int low = payload[offset + this.offset + 1] & 0xff;
        //默认大端.小端时,低字节在前.
        if (endian == Endian.Little) {
            int tmp = low;
            low = high;
            high = tmp;
        }

        int fistValue = high << 8;
        int secondValue = low;

        return (fistValue + secondValue) / 10F;
    }

    @Override
    public void encode(Float part, byte[] payload, int offset) {
        int intVal = (int) (part * 10);

        int high = intVal >> 8;
        int low = intVal & 0xff;

        if (endian == Endian.Little) {
            payload[offset + this.offset] = (byte) low;
            payload[offset + this.offset + 1] = (byte) high;
        } else {
            payload[offset + this.offset] = (byte) high;
            payload[offset + this.offset + 1] = (byte) low;
        }
    }
}
