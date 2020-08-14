package org.jetlinks.supports.protocol.codec.defaults;

import lombok.SneakyThrows;
import org.apache.commons.codec.binary.Hex;
import org.jetlinks.supports.protocol.codec.Endian;
import org.junit.Assert;
import org.junit.Test;

public class TwoBytesHexFloatCodecTest {


    @Test
    public void test() {
        TwoBytesHexFloatCodec codec = TwoBytesHexFloatCodec.of(Endian.BIG, 0);

        for (int i = -127; i < 127; i++) {
            float val = (float) (i + Math.random());
            byte[] payload = new byte[2];
            codec.encode(val, payload, 0);
//            System.out.println(String.format("%.2f = 0x%s", val, Hex.encodeHexString(payload)));
            Assert.assertEquals(codec.decode(payload, 0), val, 1);
        }

    }

    @Test
    @SneakyThrows
    public void testF() {
        TwoBytesHexFloatCodec codec = TwoBytesHexFloatCodec.of(Endian.BIG, 0);

        byte[] data = Hex.decodeHex("FF9B");

        System.out.println(codec.decode(data,0));


    }


}