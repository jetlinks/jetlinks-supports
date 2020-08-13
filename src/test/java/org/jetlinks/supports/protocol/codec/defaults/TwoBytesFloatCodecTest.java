package org.jetlinks.supports.protocol.codec.defaults;

import org.apache.commons.codec.binary.Hex;
import org.jetlinks.supports.protocol.codec.Endian;
import org.junit.Assert;
import org.junit.Test;

public class TwoBytesFloatCodecTest {

    @Test
    public void test() {
        TwoBytesFloatCodec codec = TwoBytesFloatCodec.of(Endian.BIG, 0);

        for (int i = -127; i < 127; i++) {
            float val = (float) (i + Math.random());
            byte[] payload = new byte[2];
            codec.encode(val, payload, 0);
            System.out.println(String.format("%.2f = 0x%s", val, Hex.encodeHexString(payload)));
            Assert.assertEquals(codec.decode(payload, 0), val, 1);
        }

    }




}