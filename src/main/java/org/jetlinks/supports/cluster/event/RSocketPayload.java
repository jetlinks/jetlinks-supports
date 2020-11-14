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
    public void release() {
        ReferenceCountUtil.safeRelease(payload);
    }

    @Override
    public void release(int dec) {
        ReferenceCountUtil.safeRelease(payload, dec);
    }

    @Override
    public void retain() {
        payload.retain();
    }

    @Override
    public void retain(int inc) {
        payload.retain(inc);
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
