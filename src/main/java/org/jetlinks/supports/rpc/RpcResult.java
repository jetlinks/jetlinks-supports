package org.jetlinks.supports.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import org.jetlinks.core.Payload;
import org.jetlinks.core.utils.BytesUtils;

import javax.annotation.Nonnull;

/**
 * rcp result
 */
public class RpcResult implements Payload {

    private final static Type[] types = Type.values();

    @Getter
    private final Type type;

    @Getter
    private final long requestId;

    public static RpcResult parse(Payload payload) {
        return new RpcResult(payload);
    }

    public static RpcResult complete(long messageId) {
        return new RpcResult(messageId, Type.COMPLETE, Payload.voidPayload);
    }

    public static RpcResult complete(long messageId, Payload payload) {
        return new RpcResult(messageId, Type.RESULT_AND_COMPLETE, payload);
    }

    public static RpcResult result(long messageId, Payload payload) {
        return new RpcResult(messageId, Type.RESULT, payload);
    }

    public static RpcResult error(long messageId, Payload payload) {

        return new RpcResult(messageId, Type.ERROR, payload);
    }

    private RpcResult(Payload source) {
        ByteBuf body = source.getBody();
        this.type = types[body.readByte()];
        byte[] msgId = new byte[8];
        body.getBytes(1, msgId);
        this.requestId = BytesUtils.beToLong(msgId);
        this.body = body.copy(9, body.capacity() - 9);
        body.resetReaderIndex();
    }

    private RpcResult(long requestId, Type type, Payload payload) {
        this.type = type;
        ByteBuf byteBuf = Unpooled.buffer(payload.getBody().capacity() + 9);
        byteBuf.writeByte(this.type.ordinal());
        byteBuf.writeBytes(BytesUtils.longToBe(requestId));
        byteBuf.writeBytes(payload.getBody());
        this.body = byteBuf;
        this.requestId = requestId;
        ReferenceCountUtil.safeRelease(payload);
    }

    private final ByteBuf body;

    @Nonnull
    @Override
    public ByteBuf getBody() {
        return body;
    }

    public enum Type {
        RESULT,
        COMPLETE,
        RESULT_AND_COMPLETE,
        ERROR
    }
}
