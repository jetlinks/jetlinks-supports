package org.jetlinks.supports.protocol.codec;

import org.jetlinks.supports.protocol.codec.defaults.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface BinaryCodecs {

    /**
     * 解码器
     */
    interface Decoder {

        /**
         * 返回一个固定值的decoder，无论报文是什么，都返回此固定值
         *
         * @param value 值
         * @param <T>   值类型
         * @return decoder
         */
        static <T> BinaryDecoder<T> fixed(T value) {
            return FixedValueDecoder.of(value);
        }

        /**
         * 将报文转为{@link java.util.Map}
         *
         * @param <K> Key
         * @param <V> Value
         * @return MapDecoderBuilder
         * @see MapDecoderBuilder
         */
        static <K, V> MapDecoderBuilder<K, V> map() {
            return new MapBinaryDecoder<>();
        }

        /**
         * 将指定decoder的所有结果拼接为字符串
         * <pre>
         *     builder.append(fixed("deviceId"),int4(BIG,0))
         * </pre>
         *
         * @param decoders decoder列表
         * @return decoder
         */
        static AppendBinaryDecoder append(BinaryDecoder<?>... decoders) {
            return AppendBinaryDecoder.of(decoders);
        }

    }

    /**
     * 由指定的编码解码器组成的编解码器
     *
     * @param encoder 编码器
     * @param decoder 解码器
     * @param <T>     编解码数据类型
     * @return codec
     */
    static <T> BinaryCodec<T> of(BinaryEncoder<T> encoder, BinaryDecoder<T> decoder) {
        return new BinaryCodec<T>() {
            @Override
            public T decode(byte[] payload, int offset) {
                return decoder.decode(payload, offset);
            }

            @Override
            public void encode(T part, byte[] payload, int offset) {
                encoder.encode(part, payload, offset);
            }
        };
    }

    /**
     * 将报文转换为16进制字符串
     *
     * @param offset 偏移量
     * @param length 长度,为-1时转换全部
     * @return hex codec
     */
    static BinaryCodec<String> hex(int offset, int length) {
        return HexStringCodec.of(offset, length);
    }

    /**
     * 将报文转为{@link boolean}, byte !=0 为true,否则为false
     *
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Boolean> bool(int offset) {
        return BooleanCodec.of(offset);
    }

    /**
     * 将报文转换为UTF-8字符串
     *
     * @param offset 偏移量
     * @param length 长度,如果为-1则转换全部报文
     * @return decoder
     */
    static BinaryCodec<String> string(int offset, int length) {
        return string(offset, length, StandardCharsets.UTF_8);
    }

    /**
     * 将报文转换为指定字符集的字符串
     *
     * @param offset  偏移量
     * @param length  长度,如果为-1则转换全部报文
     * @param charset 字符集
     * @return decoder
     */
    static BinaryCodec<String> string(int offset, int length, Charset charset) {
        return StringCodec.of(charset, offset, length);
    }


    /**
     * 将报文转换为{@link byte}
     *
     * @param offset 偏移量
     * @return byte codec
     */
    static BinaryCodec<Byte> int1(int offset) {
        return ByteCodec.of(offset);
    }

    /**
     * short类型decoder
     *
     * @param endian 大小端
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Short> int2(Endian endian, int offset) {
        return LongCodec
                .of(endian, offset, 2)
                .transfer(Number::shortValue, Number::longValue);
    }

    /**
     * integer类型decoder
     *
     * @param endian 大小端
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Integer> int4(Endian endian, int offset) {
        return LongCodec
                .of(endian, offset, 4)
                .transfer(Number::intValue, Number::longValue);
    }

    /**
     * long类型decoder
     *
     * @param endian 大小端
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Long> int8(Endian endian, int offset) {
        return LongCodec.of(endian, offset, 8);
    }

    /**
     * ieee754 float decoder
     *
     * @param endian 大小端
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Float> ieee754Float(Endian endian, int offset) {
        return Ieee754FloatCodec.of(endian, offset);
    }

    /**
     * ieee754 double decoder
     *
     * @param endian 大小端
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Double> ieee754Double(Endian endian, int offset) {
        return Ieee754DoubleCodec.of(endian, offset);
    }

    /**
     * 2字节 16进制转float
     * <p>
     * 算法: 2字节16进制对应的整数/10
     * <p>
     * e.g.
     * <pre>
     *     [02,22] = 0x222 = 546 , 546/10 = 54.6
     * </pre>
     *
     * @param endian 大小端
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Float> twoBytesHexFloat(Endian endian, int offset) {
        return TwoBytesHexFloatCodec.of(endian, offset);
    }

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
     * @param endian 大小端
     * @param offset 偏移量
     * @return decoder
     */
    static BinaryCodec<Float> twoBytesFloat(Endian endian, int offset) {
        return TwoBytesFloatCodec.of(endian, offset);
    }
}
