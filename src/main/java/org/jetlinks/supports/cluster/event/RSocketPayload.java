package org.jetlinks.supports.cluster.event;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import lombok.AllArgsConstructor;
import org.jetlinks.core.Payload;

import javax.annotation.Nonnull;

@AllArgsConstructor(staticName = "of")
class RSocketPayload implements Payload {

    private final io.rsocket.Payload payload;

    private final ByteBuf data;

    public static RSocketPayload of(io.rsocket.Payload payload) {
        return RSocketPayload.of(payload, payload.data());
    }

    @Override
    public Payload slice() {
        return RSocketPayload.of(payload, payload.sliceData());
    }

    @Nonnull
    @Override
    public ByteBuf getBody() {
        return data;
    }

    @Override
    public boolean release() {
        return ReferenceCountUtil.release(payload);
    }

    @Override
    public boolean release(int dec) {
        return ReferenceCountUtil.release(payload, dec);
    }

    @Override
    public RSocketPayload retain() {
        payload.retain();
        return this;
    }

    @Override
    public RSocketPayload retain(int inc) {
        payload.retain(inc);
        return this;
    }

    @Override
    public String bodyToString() {
        return payload.getDataUtf8();
    }

    @Override
    public String bodyToString(boolean release) {
        try {
            return bodyToString();
        } finally {
            if (release) {
                release();
            }
        }
    }

}
