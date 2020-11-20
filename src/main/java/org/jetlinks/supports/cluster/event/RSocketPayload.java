package org.jetlinks.supports.cluster.event;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.Payload;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

@AllArgsConstructor(staticName = "of")
@Slf4j
class RSocketPayload implements Payload {

    private final io.rsocket.Payload payload;

    private final ByteBuf data;

    public static RSocketPayload of(io.rsocket.Payload payload) {
        return RSocketPayload.of(payload, Unpooled.unreleasableBuffer(payload.data()));
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
        if (payload.refCnt() > 0) {
            return payload.release();
        }
        return true;
    }

    @Override
    public boolean release(int dec) {
        if (payload.refCnt() >= dec) {
            return payload.release(dec);
        }
        return true;
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
        return bodyToString(true);
    }

    @Override
    public String bodyToString(boolean release) {
        try {
            return data.toString(StandardCharsets.UTF_8);
        } finally {
            if (release) {
                ReferenceCountUtil.safeRelease(this);
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        int refCnt = ReferenceCountUtil.refCnt(payload);
        if (refCnt > 0) {
            log.debug("payload {} was not release properly, release() was not called before it's garbage-collected. refCnt={}", payload, refCnt);
        }
        super.finalize();
    }
}
