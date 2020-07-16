package org.jetlinks.supports.protocol.codec;

import java.util.function.Function;

public interface BinaryCodec<T> extends BinaryDecoder<T>, BinaryEncoder<T> {

    default <V> BinaryCodec<V> transfer(Function<T, V> decodeMapper, Function<V, T> encodeMapper) {

        return new BinaryCodec<V>() {
            @Override
            public V decode(byte[] payload, int offset) {
                return decodeMapper.apply(BinaryCodec.this.decode(payload, offset));
            }

            @Override
            public void encode(V part, byte[] payload, int offset) {
                BinaryCodec.this.encode(encodeMapper.apply(part), payload, offset);
            }
        };
    }
}
