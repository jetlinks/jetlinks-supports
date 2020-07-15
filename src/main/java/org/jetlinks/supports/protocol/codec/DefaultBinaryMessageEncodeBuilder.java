package org.jetlinks.supports.protocol.codec;

import lombok.AllArgsConstructor;
import org.jetlinks.core.message.CommonDeviceMessage;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.message.property.ReportPropertyMessage;

import java.util.*;
import java.util.function.BiFunction;

public class DefaultBinaryMessageEncodeBuilder implements BinaryMessageEncodeBuilder {

    private final Set<SynchronousDecoderStatement<? extends DeviceMessage>> statements = new HashSet<>();

    @Override
    public BinaryMessageDecodeStatement decode() {
        return new DefaultBinaryMessageDecodeStatement();
    }

    @Override
    public SynchronousDecoder build() {
        return new DefaultSynchronousDecoder();
    }

    @AllArgsConstructor
    private class DefaultSynchronousDecoder implements SynchronousDecoder {

        @Override
        public DeviceMessage decode(byte[] message, int offset) {

            for (SynchronousDecoderStatement<? extends DeviceMessage> statement : statements) {
                if (statement.test(message, offset)) {
                    return statement.decode(message, offset);
                }
            }

            return null;
        }
    }

    private interface SynchronousDecoderStatement<M extends DeviceMessage> extends SynchronousDecoder {

        boolean test(byte[] message, int offset);

        M decode(byte[] message, int offset);

    }

    private class DefaultBinaryMessageDecodeStatement implements SynchronousDecoderStatement<CommonDeviceMessage>, BinaryMessageDecodeStatement {

        MessagePartPredicate predicate;

        BinaryPartDecoder<String> deviceIdDecoder;

        BinaryPartDecoder<Long> timestampDecoder;

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
        public BinaryMessageDecodeStatement match(MessagePartPredicate predicate) {

            this.predicate = this.predicate == null ? predicate : this.predicate.and(predicate);

            return this;
        }

        @Override
        public BinaryMessageDecodeStatement deviceId(BinaryPartDecoder<String> part) {
            this.deviceIdDecoder = part;
            return this;
        }

        @Override
        public BinaryMessageDecodeStatement timestamp(BinaryPartDecoder<Long> decoder) {
            this.timestampDecoder = decoder;
            return this;
        }

        @Override
        @SuppressWarnings("all")
        public PropertyMessageDecodeStatement isReportProperty() {
            return (PropertyMessageDecodeStatement) (
                    this.messageSupplier = (BiFunction) new ReportPropertyBinaryMessageDecodeStatement(this)
            );
        }

        @Override
        @SuppressWarnings("all")
        public EventMessageDecodeStatement isEvent() {
            return (EventMessageDecodeStatement) (
                    this.messageSupplier = (BiFunction) new EventBinaryMessageDecodeStatement(this)
            );
        }

        public BinaryMessageDecodeStatement next() {
            statements.add(this);
            return new DefaultBinaryMessageDecodeStatement();
        }

        @Override
        public BinaryMessageEncodeBuilder end() {
            statements.add(this);
            return DefaultBinaryMessageEncodeBuilder.this;
        }

    }

    private static class ReportPropertyBinaryMessageDecodeStatement
            implements PropertyMessageDecodeStatement,
            BiFunction<byte[], Integer, ReportPropertyMessage> {

        private final Map<String, BinaryPartDecoder<?>> decoders = new HashMap<>();

        private final DefaultBinaryMessageDecodeStatement creator;

        private BinaryPartDecoder<?> messageIdDecoder;

        public ReportPropertyBinaryMessageDecodeStatement(DefaultBinaryMessageDecodeStatement creator) {
            this.creator = creator;
        }

        @Override
        public ReportPropertyMessage apply(byte[] message, Integer offset) {
            ReportPropertyMessage msg = new ReportPropertyMessage();
            if (messageIdDecoder != null) {
                msg.setMessageId(String.valueOf(messageIdDecoder.decode(message, offset)));
            }
            Map<String, Object> properties = new HashMap<>();
            for (Map.Entry<String, BinaryPartDecoder<?>> entry : decoders.entrySet()) {
                properties.put(entry.getKey(), entry.getValue().decode(message, offset));
            }
            msg.setProperties(properties);
            return msg;
        }

        @Override
        public PropertyMessageDecodeStatement property(String property, BinaryPartDecoder<?> decoder) {
            decoders.put(property, decoder);
            return this;
        }

        @Override
        public PropertyMessageDecodeStatement messageId(BinaryPartDecoder<?> decoder) {
            this.messageIdDecoder = decoder;
            return this;
        }

        @Override
        public BinaryMessageDecodeStatement next() {
            return creator.next();
        }

        @Override
        public BinaryMessageEncodeBuilder end() {
            return creator.end();
        }
    }


    private static class EventBinaryMessageDecodeStatement
            implements EventMessageDecodeStatement,
            BiFunction<byte[], Integer, EventMessage> {

        private BinaryPartDecoder<?> decoder;
        private BinaryPartDecoder<String> eventIdDecoder;

        private final DefaultBinaryMessageDecodeStatement creator;

        public EventBinaryMessageDecodeStatement(DefaultBinaryMessageDecodeStatement creator) {
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
        public BinaryMessageDecodeStatement next() {
            check();
            return creator.next();
        }

        @Override
        public EventMessageDecodeStatement eventId(BinaryPartDecoder<String> decoder) {
            this.eventIdDecoder = decoder;
            return this;
        }

        @Override
        public EventMessageDecodeStatement data(BinaryPartDecoder<?> decoder) {
            this.decoder = decoder;
            return this;
        }

        @Override
        public BinaryMessageEncodeBuilder end() {
            check();
            return creator.end();
        }

        private void check() {
            Objects.requireNonNull(decoder, "please call data(..) method before");
            Objects.requireNonNull(eventIdDecoder, "please call eventId(..) method before");
        }
    }

}
