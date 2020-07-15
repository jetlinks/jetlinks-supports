package org.jetlinks.supports.protocol.codec.defaults;

import org.jetlinks.supports.protocol.codec.BinaryPartDecoder;

import java.util.Map;

public interface MapDecoderBuilder<K, V> {

    MapDecoderBuilder<K, V> add(BinaryPartDecoder<? extends K> keyDecoder, BinaryPartDecoder<? extends V> valueDecoder);

    BinaryPartDecoder<Map<K, V>> build();
}
