package org.jetlinks.supports.protocol.codec;

public interface BinaryEncoder<T> {

    void encode(T part,
                byte[] payload,
                int offset);
}
