package org.jetlinks.supports.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.util.ResourceLeakDetector;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.defaults.ErrorCodec;
import org.jetlinks.core.codec.defaults.IntegerCodec;
import org.junit.Test;

import static org.junit.Assert.*;

public class IpcResponseTest {
    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }
    @Test
    public void testCodec() {
        IpcResponse<Integer> request = IpcResponse.of(
                ResponseType.complete,
                1234,
                1234,
                10000,
                null
        );

        ByteBuf buf = request.toByteBuf(IntegerCodec.INSTANCE, ErrorCodec.DEFAULT);

        IpcResponse<Integer> decode =
                IpcResponse.decode(Payload.of(buf), IntegerCodec.INSTANCE, ErrorCodec.DEFAULT);

        assertEquals(decode.getType(), request.getType());
        assertEquals(decode.getMessageId(), request.getMessageId());
        assertEquals(decode.getSeq(), request.getSeq());
        assertEquals(decode.getResult(), request.getResult());
    }

    @Test
    public void testCodecNoBody() {
        IpcResponse<Integer> request = IpcResponse.of(
                ResponseType.complete,
                1234,
                1234,
                null, null
        );

        ByteBuf buf = request.toByteBuf(IntegerCodec.INSTANCE, ErrorCodec.DEFAULT);

        IpcResponse<Integer> decode =
                IpcResponse.decode(Payload.of(buf), IntegerCodec.INSTANCE, ErrorCodec.DEFAULT);

        assertEquals(decode.getType(), request.getType());
        assertEquals(decode.getMessageId(), request.getMessageId());
        assertEquals(decode.getSeq(), request.getSeq());
        assertEquals(decode.getResult(), request.getResult());
    }

    @Test
    public void testError() {
        IpcResponse<Integer> request = IpcResponse.of(
                ResponseType.error,
                1234,
                1234,
                null,
                new RuntimeException("error")
        );

        ByteBuf buf = request.toByteBuf(IntegerCodec.INSTANCE, ErrorCodec.DEFAULT);

        IpcResponse<Integer> decode =
                IpcResponse.decode(Payload.of(buf), IntegerCodec.INSTANCE, ErrorCodec.DEFAULT);

        assertEquals(decode.getType(), request.getType());
        assertEquals(decode.getMessageId(), request.getMessageId());
        assertEquals(decode.getSeq(), request.getSeq());
        assertEquals(decode.getResult(), request.getResult());
        assertEquals(decode.getError().getMessage(), request.getError().getMessage());
    }
}