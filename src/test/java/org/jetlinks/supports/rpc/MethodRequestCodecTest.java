package org.jetlinks.supports.rpc;

import io.netty.util.ResourceLeakDetector;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.defaults.IntegerCodec;
import org.jetlinks.core.codec.defaults.StringCodec;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MethodRequestCodecTest {
    static {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @Test
    public void test() {
        MethodRequestCodec codec = MethodRequestCodec.of(Arrays.asList(
                IntegerCodec.INSTANCE, StringCodec.UTF8
        ));

        for (int i = 0; i < 1; i++) {
            MethodRequest request = MethodRequest.of("test", new Object[]{102, "hello"});

            Payload payload = codec.encode(request);

            MethodRequest decode = payload.decode(codec);
            assertArrayEquals(decode.getArgs(), request.getArgs());
            assertEquals(decode.getMethod(), request.getMethod());
        }

    }

}