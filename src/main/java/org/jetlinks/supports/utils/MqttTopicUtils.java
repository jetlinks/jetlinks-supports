package org.jetlinks.supports.utils;

import org.jetlinks.core.utils.TopicUtils;

import java.util.Map;


/**
 * 已弃用,使用 {@link TopicUtils}代替
 */
@Deprecated
public class MqttTopicUtils {
    public static boolean match(String topic, String target) {
        return TopicUtils.match(topic, target);

    }

    public static Map<String, String> getPathVariables(String template, String topic) {
        return TopicUtils.getPathVariables(template, topic);
    }

}
