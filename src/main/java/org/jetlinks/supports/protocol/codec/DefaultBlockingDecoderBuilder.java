package org.jetlinks.supports.protocol.codec;

import lombok.AllArgsConstructor;
import org.jetlinks.core.message.CommonDeviceMessage;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.message.property.ReportPropertyMessage;

import java.util.*;
import java.util.function.BiFunction;

class DefaultBlockingDecoderBuilder implements BlockingDecoderBuilder {

    private final Set<BlockingDecoderStatement<?>> statements = new HashSet<>();

    @Override
    public BlockingDecoderDeclaration declare() {
        return new DefaultBlockingDecoderDeclaration();
    }

    @Override
    public BlockingDecoder build() {
        return new DefaultBlockingDecoder();
    }

    @AllArgsConstructor
    private class DefaultBlockingDecoder implements BlockingDecoder {

        @Override
        public DeviceMessage decode(byte[] message, int offset) {

            for (BlockingDecoderStatement<?> statement : statements) {
                if (statement.test(message, offset)) {
                    return statement.decode(message, offset);
                }
            }

            return null;
        }
    }

    private interface BlockingDecoderStatement<M extends DeviceMessage> extends BlockingDecoder {

        boolean test(byte[] message, int offset);

        M decode(byte[] message, int offset);

    }

    private class DefaultBlockingDecoderDeclaration implements
            BlockingDecoderStatement<CommonDeviceMessage>,
            BlockingDecoderDeclaration {

        CodecPredicate predicate;

        BinaryDecoder<String> deviceIdDecoder;

        BinaryDecoder<Long> timestampDecoder;

        BiFunction<byte[], Integer, ? extends CommonDeviceMessage> messageSupplier;

        @Override
        public boolean test(byte[] message, int offset) {
            return predicate == null || predicate.test(message, offset);
        }

        @Override
        public CommonDeviceMessage decode(byte[] message, int offset) {
            CommonDeviceMessage deviceMessage = messageSupplier.apply(message, offset);
            if (timestampDecoder != null) {
                deviceMessage.setTimestamp(timestampDecoder.decode(message, offset));
            }
            if (deviceIdDecoder != null) {
                deviceMessage.setDeviceId(deviceIdDecoder.decode(message, offset));
            }
            return deviceMessage;
        }

        @Override
        public BlockingDecoderDeclaration match(CodecPredicate predicate) {

            this.predicate = this.predicate == null ? predicate : this.predicate.and(predicate);

            return this;
        }

        @Override
        public BlockingDecoderDeclaration deviceId(BinaryDecoder<String> part) {
            this.deviceIdDecoder = part;
            return this;
        }

        @Override
        public BlockingDecoderDeclaration timestamp(BinaryDecoder<Long> decoder) {
            this.timestampDecoder = decoder;
            return this;
        }

        @Override
        @SuppressWarnings("all")
        public PropertyMessageDecoderDeclaration isReportProperty() {
            return (PropertyMessageDecoderDeclaration) (
                    this.messageSupplier = (BiFunction) new ReportPropertyBinaryMessageDecoderDeclaration(this)
            );
        }

        @Override
        @SuppressWarnings("all")
        public EventMessageDecoderDeclaration isEvent() {
            return (EventMessageDecoderDeclaration) (
                    this.messageSupplier = (BiFunction) new EventBinaryMessageDecoderDeclaration(this)
            );
        }

        public BlockingDecoderDeclaration next() {
            statements.add(this);
            return new DefaultBlockingDecoderDeclaration();
        }

        @Override
        public BlockingDecoderBuilder end() {
            statements.add(this);
            return DefaultBlockingDecoderBuilder.this;
        }

    }

    private static class ReportPropertyBinaryMessageDecoderDeclaration
            implements PropertyMessageDecoderDeclaration,
            BiFunction<byte[], Integer, ReportPropertyMessage> {

        private BinaryDecoder<Map<String, Object>> decoder;

        private final DefaultBlockingDecoderDeclaration creator;

        private BinaryDecoder<?> messageIdDecoder;

        public ReportPropertyBinaryMessageDecoderDeclaration(DefaultBlockingDecoderDeclaration creator) {
            this.creator = creator;
        }

        @Override
        public ReportPropertyMessage apply(byte[] message, Integer offset) {
            ReportPropertyMessage msg = new ReportPropertyMessage();
            if (messageIdDecoder != null) {
                msg.setMessageId(String.valueOf(messageIdDecoder.decode(message, offset)));
            }
            msg.setProperties(decoder.decode(message, offset));
            return msg;
        }

        @Override
        public PropertyMessageDecoderDeclaration properties(BinaryDecoder<Map<String, Object>> decoder) {
            this.decoder = decoder;
            return this;
        }

        @Override
        public PropertyMessageDecoderDeclaration messageId(BinaryDecoder<?> decoder) {
            this.messageIdDecoder = decoder;
            return this;
        }

        @Override
        public BlockingDecoderDeclaration next() {
            Objects.requireNonNull(decoder, "please call properties(..) method before");
            return creator.next();
        }

        @Override
        public BlockingDecoderBuilder end() {
            Objects.requireNonNull(decoder, "please call properties(..) method before");
            return creator.end();
        }
    }


    private static class EventBinaryMessageDecoderDeclaration
            implements EventMessageDecoderDeclaration,
            BiFunction<byte[], Integer, EventMessage> {

        private BinaryDecoder<?> decoder;

        private BinaryDecoder<String> eventIdDecoder;

        private final DefaultBlockingDecoderDeclaration creator;

        public EventBinaryMessageDecoderDeclaration(DefaultBlockingDecoderDeclaration creator) {
            this.creator = creator;
        }

        @Override
        public EventMessage apply(byte[] message, Integer offset) {
            EventMessage msg = new EventMessage();
            msg.setEvent(eventIdDecoder.decode(message, offset));
            msg.setData(decoder.decode(message, offset));
            return msg;
        }

        @Override
        public BlockingDecoderDeclaration next() {
            check();
            return creator.next();
        }

        @Override
        public EventMessageDecoderDeclaration eventId(BinaryDecoder<String> decoder) {
            this.eventIdDecoder = decoder;
            return this;
        }

        @Override
        public EventMessageDecoderDeclaration data(BinaryDecoder<?> decoder) {
            this.decoder = decoder;
            return this;
        }

        @Override
        public BlockingDecoderBuilder end() {
            check();
            return creator.end();
        }

        private void check() {
            Objects.requireNonNull(decoder, "please call data(..) method before");
            Objects.requireNonNull(eventIdDecoder, "please call eventId(..) method before");
        }
    }

}
