package org.jetlinks.supports.protocol.codec;

import java.util.Objects;
import java.util.function.BiPredicate;

public interface CodecPredicate {

    boolean test(byte[] body, int offset);

    static <L, R> CodecPredicate of(BinaryDecoder<L> leftDecoder,
                                    BinaryDecoder<R> rightDecoder,
                                    BiPredicate<L, R> predicate) {

        return (body, offset) ->
                predicate.test(
                        leftDecoder.decode(body, offset),
                        rightDecoder.decode(body, offset)
                );
    }

    default CodecPredicate or(CodecPredicate predicate) {
        return (body, offset) -> test(body, offset) || predicate.test(body, offset);
    }

    default CodecPredicate and(CodecPredicate predicate) {
        return (body, offset) -> test(body, offset) && predicate.test(body, offset);
    }

    static <L, R extends L> CodecPredicate eq(BinaryDecoder<L> leftDecoder, BinaryDecoder<R> rightDecoder) {
        return of(leftDecoder, rightDecoder, Objects::deepEquals);
    }

    static CodecPredicate not(BinaryDecoder<?> leftDecoder, BinaryDecoder<?> rightDecoder) {
        return of(leftDecoder, rightDecoder, (l, r) -> !Objects.deepEquals(l, r));
    }

    static CodecPredicate gt(BinaryDecoder<Number> leftDecoder, BinaryDecoder<Number> rightDecoder) {
        return of(leftDecoder, rightDecoder, (l, r) -> l.doubleValue() > r.doubleValue());
    }

    static CodecPredicate lt(BinaryDecoder<Number> leftDecoder, BinaryDecoder<Number> rightDecoder) {
        return of(leftDecoder, rightDecoder, (l, r) -> l.doubleValue() < r.doubleValue());
    }

    static CodecPredicate lte(BinaryDecoder<Number> leftDecoder, BinaryDecoder<Number> rightDecoder) {
        return of(leftDecoder, rightDecoder, (l, r) -> l.doubleValue() <= r.doubleValue());
    }

    static CodecPredicate gte(BinaryDecoder<Number> leftDecoder, BinaryDecoder<Number> rightDecoder) {
        return of(leftDecoder, rightDecoder, (l, r) -> l.doubleValue() >= r.doubleValue());
    }

}
