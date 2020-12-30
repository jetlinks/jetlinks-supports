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

@AllArgsConstructor(staticName = "of")
@Getter
class IpcResponse<T> {

    private final static ResponseType[] types = ResponseType.values();

    private final ResponseType type;
    private final int seq;
    private final int messageId;
    private final T result;
    private final Throwable error;


    boolean hasResult() {
        return result != null;
    }

    boolean hasError() {
        return error != null;
    }

    public static <T> IpcResponse<T> decode(Payload payload, Decoder<T> decoder, Decoder<Throwable> errorDecoder) {
        ByteBuf body = payload.getBody();
        try {
            byte type = body.readByte();
            if (type < 0 || type >= types.length) {
                throw new IllegalStateException("unknown request type " + type);
            }
            ResponseType requestType = types[type];

            int seq = body.readInt();
            int messageId = body.readInt();
            boolean hasBody = body.readByte() == 1;
            T requestBody = null;
            Throwable error = null;
            if (hasBody && requestType != ResponseType.error) {
                requestBody = decoder.decode(payload);
            }
            if (requestType == ResponseType.error) {
                error = errorDecoder.decode(payload);
            }
            return IpcResponse.of(requestType, seq, messageId, requestBody, error);
        } finally {
            ReferenceCountUtil.safeRelease(payload);
        }
    }

    public ByteBuf toByteBuf(Encoder<T> encoder, Encoder<Throwable> errorEncoder) {
        ByteBuf buf;
        ByteBuf body;
        ReferenceCounted ref;
        if (result == null && error == null) {
            buf = ByteBufAllocator.DEFAULT.buffer(9);
            body = Unpooled.EMPTY_BUFFER;
            ref = body;
        } else {
            Payload payload = result == null ? errorEncoder.encode(error) : encoder.encode(result);
            body = payload.getBody();
            buf = ByteBufAllocator.DEFAULT.buffer(9 + body.writerIndex());
            ref = payload;
        }
        buf.writeByte(type.ordinal());//请求类型
        buf.writeInt(seq);//seq
        buf.writeInt(messageId);//messageId
        buf.writeByte((result == null && error == null) ? 0 : 1);//hasBody
        buf.writeBytes(body);//请求体
        ReferenceCountUtil.safeRelease(ref);
        return buf;
    }

    @Override
    public String toString() {
        return "IpcResponse{" +
                "type=" + type +
                ", seq=" + seq +
                ", messageId=" + messageId +
                '}';
    }
}
