package org.jetlinks.supports.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.Decoder;
import org.jetlinks.core.codec.Encoder;
import org.jetlinks.core.utils.BytesUtils;

@AllArgsConstructor(staticName = "of")
@Getter
public class IpcRequest<T> {

    private final static RequestType[] types = RequestType.values();

    private final RequestType type;
    private final int consumerId;
    private final int messageId;
    private final int seq;
    private final T request;

    public static <T> IpcRequest<T> of(RequestType type, int consumerId, int messageId, T request) {
        return of(type, consumerId, messageId, -1, request);
    }

    public static <T> IpcRequest<T> decode(Payload payload, Decoder<T> decoder) {
        try {
            ByteBuf body = payload.getBody();
            byte type = body.readByte();
            if (type < 0 || type >= types.length) {
                throw new IllegalStateException("unknown request type " + type);
            }
            RequestType requestType = types[type];
            byte[] intArray = new byte[4];
            body.readBytes(intArray);
            int invokeId = BytesUtils.beToInt(intArray);
            body.readBytes(intArray);
            int messageId = BytesUtils.beToInt(intArray);
            body.readBytes(intArray);
            int seq = BytesUtils.beToInt(intArray);
            boolean hasBody = body.readByte() == 1;
            T requestBody = null;
            if (hasBody) {
                requestBody = decoder.decode(payload);
            }
            return IpcRequest.of(requestType, invokeId, messageId, seq, requestBody);
        } finally {
            ReferenceCountUtil.safeRelease(payload);
        }
    }

    public ByteBuf toByteBuf(Encoder<T> encoder) {
        ByteBuf buf;
        ByteBuf body;
        ReferenceCounted ref;
        if (request == null) {
            buf = ByteBufAllocator.DEFAULT.buffer(9);
            body = Unpooled.EMPTY_BUFFER;
            ref = body;
        } else {
            Payload payload = encoder.encode(request);
            body = payload.getBody();
            buf = ByteBufAllocator.DEFAULT.buffer(9 + body.writerIndex());
            ref = payload;
        }
        buf.writeByte(type.ordinal());//请求类型
        buf.writeBytes(BytesUtils.intToBe(consumerId));//invokerId
        buf.writeBytes(BytesUtils.intToBe(messageId));//messageId
        buf.writeBytes(BytesUtils.intToBe(seq));//seq
        buf.writeByte(request == null ? 0 : 1);//hasBody
        buf.writeBytes(body);//请求体
        ReferenceCountUtil.safeRelease(ref);
        return buf;
    }
}
