package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import org.jetlinks.core.things.ThingMetadata;
import org.jetlinks.core.things.ThingMetadataCodec;
import reactor.core.publisher.Mono;

public class DefaultThingMetadataCodec implements ThingMetadataCodec {

    private static final DefaultThingMetadataCodec INSTANCE = new DefaultThingMetadataCodec();

    public static DefaultThingMetadataCodec getInstance() {
        return INSTANCE;
    }

    public ThingMetadata doDecode(String json) {
        return new DefaultThingsMetadata(JSON.parseObject(json));
    }

    @Override
    public Mono<ThingMetadata> decode(String source) {
        return Mono.just(doDecode(source));
    }

    public String doEncode(ThingMetadata metadata) {
        return new DefaultThingsMetadata(metadata).toJson().toJSONString();
    }

    @Override
    public Mono<String> encode(ThingMetadata metadata) {
        return Mono.just(doEncode(metadata));
    }
}
