package org.jetlinks.supports.protocol.codec;

import lombok.AllArgsConstructor;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

@AllArgsConstructor
public class BlockingDeviceMessageCodec implements DeviceMessageCodec {

    private final Transport transport;

    private final BlockingDecoder decoder;

    private final BlockingEncoder encoder;

    @Override
    public Transport getSupportTransport() {
        return transport;
    }

    @Nonnull
    @Override
    public Publisher<? extends Message> decode(@Nonnull MessageDecodeContext context) {
        return Mono.fromCallable(() -> {
            byte[] payload = context.getMessage().payloadAsBytes();
            return decoder.decode(payload, 0);
        });
    }

    @Nonnull
    @Override
    public Publisher<? extends EncodedMessage> encode(@Nonnull MessageEncodeContext context) {
        return Mono.fromCallable(() -> encoder.encode(context));
    }
}
