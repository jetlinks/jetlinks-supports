package org.jetlinks.supports.protocol.codec;

import java.util.Map;

/**
 * <pre>
 *
 *  BinaryMessageCodecBuilder.create()
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
public interface BlockingDecoderBuilder {

    static BlockingDecoderBuilder create() {
        return new DefaultBlockingDecoderBuilder();
    }

    BlockingDecoderDeclaration declare();

    BlockingDecoder build();

    interface BlockingDecoderDeclaration {

        BlockingDecoderDeclaration match(CodecPredicate predicate);

        BlockingDecoderDeclaration deviceId(BinaryDecoder<String> decoder);

        BlockingDecoderDeclaration timestamp(BinaryDecoder<Long> decoder);

        PropertyMessageDecoderDeclaration isReportProperty();

        EventMessageDecoderDeclaration isEvent();

        BlockingDecoderBuilder end();

    }

    interface PropertyMessageDecoderDeclaration {

        PropertyMessageDecoderDeclaration properties(BinaryDecoder<Map<String,Object>> decoder);

        PropertyMessageDecoderDeclaration messageId(BinaryDecoder<?> decoder);

        BlockingDecoderDeclaration next();

        BlockingDecoderBuilder end();

    }

    interface EventMessageDecoderDeclaration {

        EventMessageDecoderDeclaration eventId(BinaryDecoder<String> eventIdDecoder);

        EventMessageDecoderDeclaration data(BinaryDecoder<?> decoder);

        BlockingDecoderDeclaration next();

        BlockingDecoderBuilder end();

    }

}
