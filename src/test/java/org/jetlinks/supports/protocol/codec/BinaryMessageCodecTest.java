package org.jetlinks.supports.protocol.codec;

import org.jetlinks.core.message.DeviceMessage;
import org.junit.Test;

import static org.jetlinks.supports.protocol.codec.BinaryCodecs.*;
import static org.jetlinks.supports.protocol.codec.BinaryCodecs.Decoder.*;
import static org.jetlinks.supports.protocol.codec.CodecPredicate.eq;

public class BinaryMessageCodecTest {

    @Test
    public void test() {

        byte[] data = {
                0x01, 0x04, 0x02, 0x02, 0x22, 0x01, 0x41
        };

        BlockingDecoder decoder = BlockingDecoderBuilder.create()
                .declare()
                // 0x04 属性上报
                .match(eq(int1(1), fixed((byte) 0x04)))
                .deviceId(append(fixed("device-"), int1(0)))
                .isReportProperty()
                .properties(
                        Decoder.<String, Object>map()
                                .add(fixed("humidity"), twoBytesHexFloat(Endian.BIG, 3))
                                .add(fixed("temperature"), twoBytesHexFloat(Endian.BIG, 5))
                                .build()
                )
                .next()
                // 0x01 事件上报
                .match(eq(int1(1), fixed((byte) 0x01)))
                .deviceId(append(fixed("device-"), int1(0)))
                .isEvent()
                .eventId(fixed("fire_alarm"))
                .data(map()
                        .add(fixed("temp"), twoBytesHexFloat(Endian.BIG, 3))
                        .build())
                .end()
                .build();

        DeviceMessage message = decoder.decode(data, 0);

        System.out.println(message);

    }

}