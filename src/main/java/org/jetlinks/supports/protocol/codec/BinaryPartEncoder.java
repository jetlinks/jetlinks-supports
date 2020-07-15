package org.jetlinks.supports.protocol.codec;

public interface BinaryPartEncoder<T> {

    void encode(T part,
                byte[] payload,
                int offset);
}
