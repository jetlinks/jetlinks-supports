package org.jetlinks.supports.utils;

import org.jetlinks.core.utils.TopicUtils;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

import java.util.HashMap;
import java.util.Map;


/**
 * 已弃用,使用 {@link TopicUtils}代替
 */
@Deprecated
public class MqttTopicUtils {
    private final static PathMatcher pathMatcher = new AntPathMatcher();

    public static boolean match(String topic, String target) {

        if (topic.equals(target)) {
            return true;
        }

        if (!topic.contains("#") && !topic.contains("+") && !topic.contains("{")) {
            return false;
        }

        return pathMatcher.match(topic.replace("#", "**").replace("+", "*"), target);

    }

    public static Map<String, String> getPathVariables(String template, String topic) {
        try {
            return pathMatcher.extractUriTemplateVariables(template, topic);
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

}
