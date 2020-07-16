package org.jetlinks.supports.protocol.codec.defaults;

import org.jetlinks.supports.protocol.codec.BinaryDecoder;

import java.util.Map;

public interface MapDecoderBuilder<K, V> {

    MapDecoderBuilder<K, V> add(BinaryDecoder<? extends K> keyDecoder,
                                BinaryDecoder<? extends V> valueDecoder);

    BinaryDecoder<Map<K, V>> build();
}
