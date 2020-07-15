package org.jetlinks.supports.protocol.codec;

import java.util.function.Function;

public interface BinaryPartDecoder<T> {

    T decode(byte[] payload, int offset);

    default <V> BinaryPartDecoder<V> transferTo(Function<T, V> mapper) {
        return (payload, offset) -> mapper.apply(decode(payload, offset));
    }

}
