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

    private io.rsocket.Payload payload;

    private ByteBuf data;

    public static RSocketPayload of(io.rsocket.Payload payload) {
        return RSocketPayload.of(payload, Unpooled.unreleasableBuffer(payload.data()));
    }

    @Override
    public Payload slice() {
        if (payload == null) {
            throw new IllegalStateException("payload released");
        }
        return RSocketPayload.of(payload, data.slice());
    }

    @Nonnull
    @Override
    public ByteBuf getBody() {
        if (data == null) {
            throw new IllegalStateException("payload released");
        }
        return data;
    }

    @Override
    public boolean release() {
        return payload == null || handleRelease(ReferenceCountUtil.release(payload));
    }

    @Override
    public boolean release(int dec) {
        return payload == null || handleRelease(ReferenceCountUtil.release(payload, dec));
    }

    protected boolean handleRelease(boolean released) {
        if (released) {
            payload = null;
            data = null;
        }
        return released;
    }

    @Override
    public RSocketPayload retain() {
        ReferenceCountUtil.retain(payload);
        return this;
    }

    @Override
    public RSocketPayload retain(int inc) {
        ReferenceCountUtil.retain(payload, inc);
        return this;
    }

    @Override
    public int refCnt() {
        return ReferenceCountUtil.refCnt(payload);
    }

    @Override
    public String bodyToString() {
        return bodyToString(true);
    }

    @Override
    public String bodyToString(boolean release) {
        try {
            if (payload == null) {
                throw new IllegalStateException("payload released");
            }
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
