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
    private final long requesterId;

    @Getter
    private final long requestId;

    @Getter
    private final ByteBuf body;

    public static RpcRequest parse(Payload payload) {
        return new RpcRequest(payload);
    }

    public static RpcRequest next(long requesterId, long requestId, Payload payload) {
        return new RpcRequest(Type.NEXT, requesterId, requestId, payload);
    }

    public static RpcRequest complete(long requesterId, long requestId) {
        return new RpcRequest(Type.COMPLETE, requesterId, requestId, voidPayload);
    }

    public static RpcRequest nextAndComplete(long requesterId, long requestId, Payload payload) {
        return new RpcRequest(Type.NEXT_AND_END, requesterId, requestId, payload);
    }

    private RpcRequest(Type type, long requesterId, long requestId, Payload payload) {
        try {
            ByteBuf body = payload.getBody();
            ByteBuf byteBuf = Unpooled.buffer(17 + body.writerIndex());

            byteBuf.writeByte(type.ordinal());
            byteBuf.writeBytes(BytesUtils.longToBe(requestId));
            byteBuf.writeBytes(BytesUtils.longToBe(requesterId));

            byteBuf.writeBytes(body);

            this.type = type;
            this.body = byteBuf;
            this.requestId = requestId;
            this.requesterId = requesterId;
        } finally {
            ReferenceCountUtil.safeRelease(payload);
        }
    }

    private RpcRequest(Payload payload) {
        ByteBuf byteBuf = payload.getBody();
        this.type = types[byteBuf.readByte()];
        byte[] msgId = new byte[8];
        byte[] reqId = new byte[8];
        byteBuf.getBytes(1, msgId);
        byteBuf.getBytes(9, reqId);
        this.requestId = BytesUtils.beToLong(msgId);
        this.requesterId = BytesUtils.beToLong(reqId);
        this.body = byteBuf.copy(17, byteBuf.writerIndex() - 17);
        byteBuf.resetReaderIndex();
        ReferenceCountUtil.safeRelease(payload);
    }

    public enum Type {
        NEXT,
        COMPLETE,
        NEXT_AND_END
    }
}
