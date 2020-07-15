package org.jetlinks.supports.protocol.codec;

/**
 * <pre>
 *
 * SynchronousDecoder decoder = BinaryMessageCodecBuilder.create()
 *      .decode()
 *      // 0x04 属性上报
 *      .match(eq(int1(1), fixed((byte) 0x04)))
 *      .deviceId(append(string("device-"), int1(0)))
 *      .isReportProperty()
 *      .property("humidity", hexFloat(Endian.BIG, 3))
 *      .property("temperature", hexFloat(Endian.BIG, 5))
 *
 *      .next()
 *      // 0x01 事件上报
 *      .match(eq(int1(1), fixed((byte) 0x01)))
 *      .deviceId(append(string("device-"), int1(0)))
 *      .isEvent()
 *      .eventId(fixed("fire_alarm"))
 *      .data(map()
 *      .add(fixed("temp"), hexFloat(Endian.BIG, 3))
 *      .build())
 *
 *      .end()
 *      .build();
 *
 * </pre>
 *
 * @see BinaryCodecs
 * @since 1.1.1
 */
public interface BinaryMessageEncodeBuilder {

    static BinaryMessageEncodeBuilder create() {
        return new DefaultBinaryMessageEncodeBuilder();
    }

    BinaryMessageDecodeStatement decode();

    SynchronousDecoder build();

    interface BinaryMessageDecodeStatement {

        BinaryMessageDecodeStatement match(MessagePartPredicate predicate);

        BinaryMessageDecodeStatement deviceId(BinaryPartDecoder<String> decoder);

        BinaryMessageDecodeStatement timestamp(BinaryPartDecoder<Long> decoder);

        PropertyMessageDecodeStatement isReportProperty();

        EventMessageDecodeStatement isEvent();

        BinaryMessageEncodeBuilder end();

    }

    interface PropertyMessageDecodeStatement {

        PropertyMessageDecodeStatement property(String property, BinaryPartDecoder<?> decoder);

        PropertyMessageDecodeStatement messageId(BinaryPartDecoder<?> decoder);

        BinaryMessageDecodeStatement next();

        BinaryMessageEncodeBuilder end();

    }

    interface EventMessageDecodeStatement {

        EventMessageDecodeStatement eventId(BinaryPartDecoder<String> eventIdDecoder);

        EventMessageDecodeStatement data(BinaryPartDecoder<?> decoder);

        BinaryMessageDecodeStatement next();

        BinaryMessageEncodeBuilder end();

    }

}
