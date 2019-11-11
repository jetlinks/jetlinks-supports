package org.jetlinks.supports.utils;

import org.junit.Assert;
import org.junit.Test;

public class MqttTopicUtilsTest {


    @Test
    public void testMatch() {

        Assert.assertTrue(MqttTopicUtils.match("/test", "/test"));

        Assert.assertFalse(MqttTopicUtils.match("/test", "/test/a"));


        Assert.assertTrue(MqttTopicUtils.match("/test/#", "/test/a"));
        Assert.assertTrue(MqttTopicUtils.match("/test/#", "/test/a/b/c"));

        Assert.assertFalse(MqttTopicUtils.match("/test/+/", "/test/a/b/c"));

        Assert.assertTrue(MqttTopicUtils.match("/test/+", "/test/a"));

    }

    @Test
    public void testVar(){
        Assert.assertEquals(MqttTopicUtils.getPathVariables("/test/{userId}/test","/test/123/test").get("userId"),"123");

        Assert.assertEquals(MqttTopicUtils.getPathVariables("/test/{userId:.+}/test","/test/123/test").get("userId"),"123");

        Assert.assertNull(MqttTopicUtils.getPathVariables("/testa/{userId:.+}/test", "/test/123/test").get("userId"));

    }
}