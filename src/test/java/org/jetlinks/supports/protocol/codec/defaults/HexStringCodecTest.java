package org.jetlinks.supports.protocol.codec.defaults;

import org.junit.Test;

import static org.junit.Assert.*;

public class HexStringCodecTest {


    @Test
    public void test() {
        HexStringCodec codec = HexStringCodec.of(0, 10);

        byte[] data = new byte[2];
        codec.encode("10ff", data, 0);

        assertEquals(codec.decode(data,0),"10ff");
    }

}