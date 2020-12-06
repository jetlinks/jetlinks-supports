package org.jetlinks.supports.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.util.ResourceLeakDetector;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.defaults.IntegerCodec;
import org.junit.Test;

import static org.junit.Assert.*;

public class IpcRequestTest {

    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }
    @Test
    public void testCodec() {
        IpcRequest<Integer> request = IpcRequest.of(
                RequestType.request,
                1234,
                1234,
                10000
        );

        ByteBuf buf = request.toByteBuf(IntegerCodec.INSTANCE);

        IpcRequest<Integer> decode =
                IpcRequest.decode(Payload.of(buf),IntegerCodec.INSTANCE);

        assertEquals(decode.getType(),request.getType());
        assertEquals(decode.getMessageId(),request.getMessageId());
        assertEquals(decode.getRequest(),request.getRequest());
        assertEquals(decode.getConsumerId(), request.getConsumerId());
    }

    @Test
    public void testCodecNoBody() {
        IpcRequest<Integer> request = IpcRequest.of(
                RequestType.request,
                1234,
                1234,
                null
        );

        ByteBuf buf = request.toByteBuf(IntegerCodec.INSTANCE);

        IpcRequest<Integer> decode =
                IpcRequest.decode(Payload.of(buf),IntegerCodec.INSTANCE);

        assertEquals(decode.getType(),request.getType());
        assertEquals(decode.getMessageId(),request.getMessageId());
        assertEquals(decode.getRequest(),request.getRequest());
        assertEquals(decode.getConsumerId(), request.getConsumerId());
    }
}