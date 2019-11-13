package org.jetlinks.supports.protocol;

import lombok.Builder;
import lombok.Getter;
import org.jetlinks.core.message.codec.MessagePayloadType;
import org.jetlinks.core.message.codec.MqttMessageCodecDescription;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultMqttMessageCodecDescription implements MqttMessageCodecDescription {
    private List<Topic> upstream = new ArrayList<>();

    private List<Topic> downstream = new ArrayList<>();

    @Override
    public Flux<Topic> getUpStreamTopics() {

        return Flux.fromIterable(upstream);
    }

    @Override
    public Flux<Topic> getDownStreamTopics() {

        return Flux.fromIterable(downstream);
    }


    public DefaultMqttMessageCodecDescription addUpstream(DefaultTopic.Builder builder) {
        upstream.add(builder.build());
        return this;
    }

    public DefaultMqttMessageCodecDescription addDownstream(DefaultTopic.Builder builder) {
        downstream.add(builder.build());
        return this;
    }

    @Getter
    @Builder(builderClassName = "Builder")
    public static class DefaultTopic implements Topic {

        private String topic;

        private List<String> variables;

        private MessagePayloadType payloadType;

        private String templatePayload;

        private String description;

        @Override
        public MessagePayloadType getPayloadType() {
            return payloadType;
        }

        @Override
        public String getTopic() {
            return topic;
        }

        @Override
        public String getTopic(Map<String, Object> variables) {

            String tp = topic;
            for (String variable : this.variables) {
                tp = tp.replace("{".concat(variable).concat("}"), String.valueOf(variables.get(variable)));
            }
            return tp;
        }

    }
}
