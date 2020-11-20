package org.jetlinks.supports.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import org.jetlinks.core.Payload;
import org.jetlinks.core.utils.BytesUtils;

public class RpcRequest implements Payload {

    private static final Type[] types = Type.values();

    @Getter
    private final Type type;

    @Getter
    private final long requestId;

    @Getter
    private final ByteBuf body;

    public static RpcRequest parse(Payload payload) {
        return new RpcRequest(payload);
    }

    public static RpcRequest next(long requestId, Payload payload) {
        return new RpcRequest(Type.NEXT, requestId, payload);
    }

    public static RpcRequest complete(long requestId) {
        return new RpcRequest(Type.COMPLETE, requestId, voidPayload);
    }

    public static RpcRequest nextAndComplete(long requestId, Payload payload) {
        return new RpcRequest(Type.NEXT_AND_END, requestId, payload);
    }

    private RpcRequest(Type type, long requestId, Payload payload) {
        try {
            ByteBuf body = payload.getBody();
            ByteBuf byteBuf = Unpooled.buffer(9 +body.capacity());

            byteBuf.writeByte(type.ordinal());
            byteBuf.writeBytes(BytesUtils.longToBe(requestId));
            byteBuf.writeBytes(body);

            this.type = type;
            this.body = byteBuf;
            this.requestId = requestId;
        } finally {
            ReferenceCountUtil.safeRelease(payload);
        }
    }

    private RpcRequest(Payload payload) {
        ByteBuf byteBuf = payload.getBody();
        this.type = types[byteBuf.readByte()];
        byte[] msgId = new byte[8];
        byteBuf.getBytes(1, msgId);
        this.requestId = BytesUtils.beToLong(msgId);
        this.body = byteBuf.copy(9, byteBuf.capacity() - 9);
        byteBuf.resetReaderIndex();
        ReferenceCountUtil.safeRelease(payload);
    }

    public enum Type {
        NEXT,
        COMPLETE,
        NEXT_AND_END
    }
}
