package org.jetlinks.supports.protocol.codec;

import java.util.function.Function;

public interface BinaryDecoder<T> {

    T decode(byte[] payload, int offset);

    default <V> BinaryDecoder<V> transferTo(Function<T, V> mapper) {
        return (payload, offset) -> mapper.apply(decode(payload, offset));
    }

}
