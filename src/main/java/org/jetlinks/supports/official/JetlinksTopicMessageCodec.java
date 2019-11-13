package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.core.message.property.WritePropertyMessage;
import org.jetlinks.core.message.property.WritePropertyMessageReply;
import org.jetlinks.supports.utils.MqttTopicUtils;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Optional;

class JetlinksTopicMessageCodec {

    @Getter
    protected class DecodeResult {
        private Map<String, String> args;

        private boolean child;

        private boolean event;
        private boolean readPropertyReply;
        private boolean writePropertyReply;
        private boolean functionInvokeReply;
        private boolean reportProperties;
        private boolean derivedMetadata;

        public DecodeResult(String topic) {
            this.topic = topic;
            args = MqttTopicUtils.getPathVariables("/{productId}/{deviceId}/**", topic);
            if (topic.contains("child")) {
                child = true;
                args.putAll(MqttTopicUtils.getPathVariables("/**/child/{childDeviceId}/**", topic));
            }
            if (topic.contains("event")) {
                event = true;
                args.putAll(MqttTopicUtils.getPathVariables("/**/event/{eventId}", topic));
            }
            derivedMetadata = topic.endsWith("metadata/derived");

            event = event || derivedMetadata || (reportProperties = topic.endsWith("properties/report"));

            readPropertyReply = topic.endsWith("properties/read/reply");
            writePropertyReply = topic.endsWith("properties/write/reply");
            functionInvokeReply = topic.endsWith("function/invoke/reply");

        }

        private String topic;

        public String getDeviceId() {
            return args.get("deviceId");
        }

        public String getChildDeviceId() {
            return args.get("childDeviceId");
        }

        protected Message message;
    }

    protected EncodedTopic encode(String deviceId, Message message) {

        Assert.hasText(deviceId,"deviceId can not be null");
        Assert.notNull(message,"message can not be null");

        if (message instanceof ReadPropertyMessage) {
            String topic = "/".concat(deviceId).concat("/properties/read");
            JSONObject mqttData = new JSONObject();
            mqttData.put("messageId", message.getMessageId());
            mqttData.put("properties", ((ReadPropertyMessage) message).getProperties());
            mqttData.put("deviceId", deviceId);

            return new EncodedTopic(topic, mqttData);
        } else if (message instanceof WritePropertyMessage) {
            String topic = "/".concat(deviceId).concat("/properties/write");
            JSONObject mqttData = new JSONObject();
            mqttData.put("messageId", message.getMessageId());
            mqttData.put("properties", ((WritePropertyMessage) message).getProperties());
            mqttData.put("deviceId", deviceId);

            return new EncodedTopic(topic, mqttData);
        } else if (message instanceof FunctionInvokeMessage) {
            String topic = "/".concat(deviceId).concat("/function/invoke");
            FunctionInvokeMessage invokeMessage = ((FunctionInvokeMessage) message);
            JSONObject mqttData = new JSONObject();
            mqttData.put("messageId", message.getMessageId());
            mqttData.put("function", invokeMessage.getFunctionId());
            mqttData.put("inputs", invokeMessage.getInputs());
            mqttData.put("deviceId", deviceId);

            return new EncodedTopic(topic, mqttData);
        } else if (message instanceof ChildDeviceMessage) {
            ChildDeviceMessage childDeviceMessage = ((ChildDeviceMessage) message);
            EncodedTopic result = encode(childDeviceMessage.getChildDeviceId(), childDeviceMessage.getChildDeviceMessage());
            String topic = "/".concat(deviceId).concat("/child").concat(result.topic);
            result.payload.put("deviceId", childDeviceMessage.getChildDeviceId());

            return new EncodedTopic(topic, result.payload);
        }
        throw new UnsupportedOperationException("unsupported message type :" + message.getClass());
    }

    protected DecodeResult decode(String topic, JSONObject object) {
        DecodeResult result = new DecodeResult(topic);
        Message message = null;
        if (result.isEvent()) {
            message = decodeEvent(result, object);
        } else if (result.isReadPropertyReply()) {
            message = decodeReadPropertyReply(result, object);
        } else if (result.isWritePropertyReply()) {
            message = decodeWritePropertyReply(result, object);
        } else if (result.isFunctionInvokeReply()) {
            message = decodeInvokeReply(result, object);
        }

        if (result.isChild()) {
            if (topic.endsWith("connected")) {
                message = object.toJavaObject(DeviceOnlineMessage.class);
            } else if (topic.endsWith("disconnect")) {
                message = object.toJavaObject(DeviceOfflineMessage.class);
            }
            if (message == null) {
                throw new UnsupportedOperationException("unsupported topic:" + topic);
            }
            applyCommons(message, result, object);
            ChildDeviceMessageReply children = new ChildDeviceMessageReply();
            children.setChildDeviceId(result.getChildDeviceId());
            children.setDeviceId(result.getDeviceId());
            children.setChildDeviceMessage(message);
            children.setSuccess(Optional.ofNullable(object.getBoolean("success")).orElse(true));
            children.setTimestamp(Optional.ofNullable(object.getLong("timestamp")).orElse(System.currentTimeMillis()));
            Optional.ofNullable(object.getString("messageId")).ifPresent(children::setMessageId);
            result.message = children;
        } else {
            if (message == null) {
                throw new UnsupportedOperationException("unsupported topic:" + topic);
            }
            applyCommons(message, result, object);
            result.message = message;
        }
        return result;
    }


    private Message decodeEvent(DecodeResult result, JSONObject event) {
        EventMessage message = event.toJavaObject(EventMessage.class);
        message.setData(event.get("data"));
        message.setEvent(result.args.get("eventId"));
        if (result.isReportProperties()) {
            message.addHeader(Headers.reportProperties, true);
        }
        if (result.isDerivedMetadata()) {
            message.addHeader(Headers.reportDerivedMetadata, true);
        }
        if (result.isChild()) {
            message.setDeviceId(result.getChildDeviceId());
        } else {
            message.setDeviceId(result.getDeviceId());
        }
        message.setSuccess(Optional.ofNullable(event.getBoolean("success")).orElse(true));
        return message;
    }

    private Message decodeReadPropertyReply(DecodeResult result, JSONObject data) {

        return data.toJavaObject(ReadPropertyMessageReply.class);
    }

    private Message decodeWritePropertyReply(DecodeResult result, JSONObject data) {

        return data.toJavaObject(WritePropertyMessageReply.class);
    }

    private Message decodeInvokeReply(DecodeResult result, JSONObject data) {
        return data.toJavaObject(FunctionInvokeMessageReply.class);
    }

    private void applyCommons(Message message, DecodeResult result, JSONObject data) {
        if (message instanceof CommonDeviceMessageReply) {
            CommonDeviceMessageReply reply = ((CommonDeviceMessageReply) message);
            reply.setSuccess(Optional.ofNullable(data.getBoolean("success")).orElse(true));
            reply.setTimestamp(Optional.ofNullable(data.getLong("timestamp")).orElse(System.currentTimeMillis()));
            if (result.isChild()) {
                reply.setDeviceId(result.getChildDeviceId());
            } else {
                reply.setDeviceId(result.getDeviceId());
            }
        }
        if (message instanceof CommonDeviceMessage) {
            CommonDeviceMessage msg = ((CommonDeviceMessage) message);
            msg.setTimestamp(Optional.ofNullable(data.getLong("timestamp")).orElse(System.currentTimeMillis()));
            if (result.isChild()) {
                msg.setDeviceId(result.getChildDeviceId());
            } else {
                msg.setDeviceId(result.getDeviceId());
            }
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    protected class EncodedTopic {
        String topic;

        JSONObject payload;
    }

    @Getter
    @Setter
    protected class Decoded {
        Message message;

    }

}
