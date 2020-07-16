package org.jetlinks.supports.protocol.codec;

import java.util.Objects;
import java.util.function.BiPredicate;

public interface MessagePartPredicate {

    boolean test(byte[] body, int offset);

    default MessagePartPredicate or(MessagePartPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    default MessagePartPredicate and(MessagePartPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    static MessagePartPredicate of(BinaryDecoder<?> leftDecoder,
                                   BinaryDecoder<?> rightDecoder,
                                   BiPredicate<Object, Object> predicate) {

        return (body, offset) ->
                predicate.test(
                        leftDecoder.decode(body, offset),
                        rightDecoder.decode(body, offset)
                );
    }

    static <L,R extends L> MessagePartPredicate eq(BinaryDecoder<L> leftDecoder, BinaryDecoder<R> rightDecoder) {
        return of(leftDecoder, rightDecoder, Objects::deepEquals);
    }

    static MessagePartPredicate gt(BinaryDecoder<?> leftDecoder, BinaryDecoder<?> rightDecoder) {
        throw new UnsupportedOperationException();
    }

    static MessagePartPredicate lt(BinaryDecoder<?> leftDecoder, BinaryDecoder<?> rightDecoder) {
        throw new UnsupportedOperationException();
    }
}
