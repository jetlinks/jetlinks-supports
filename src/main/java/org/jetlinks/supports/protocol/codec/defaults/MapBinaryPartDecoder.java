package org.jetlinks.supports.protocol.codec.defaults;

import lombok.AllArgsConstructor;
import org.jetlinks.supports.protocol.codec.BinaryPartDecoder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapBinaryPartDecoder<K, V> implements BinaryPartDecoder<Map<K, V>>, MapDecoderBuilder<K, V> {

    private final List<Decoder<K, V>> decoders = new ArrayList<>();

    @Override
    public Map<K, V> decode(byte[] payload, int offset) {
        Map<K, V> map = new LinkedHashMap<>();

        for (Decoder<K, V> decoder : decoders) {
            map.put(decoder.keyDecoder.decode(payload, offset), decoder.valueDecoder.decode(payload, offset));
        }

        return map;
    }

    @Override
    public MapDecoderBuilder<K, V> add(BinaryPartDecoder<? extends K> keyDecoder, BinaryPartDecoder<? extends V> valueDecoder) {
        decoders.add(new Decoder<>(keyDecoder, valueDecoder));
        return this;
    }

    @Override
    public BinaryPartDecoder<Map<K, V>> build() {
        return this;
    }

    @AllArgsConstructor
    private static class Decoder<K, V> {
        private final BinaryPartDecoder<? extends K> keyDecoder;
        private final BinaryPartDecoder<? extends V> valueDecoder;
    }
}
