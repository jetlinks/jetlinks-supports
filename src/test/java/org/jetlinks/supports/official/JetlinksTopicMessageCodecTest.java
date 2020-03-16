package org.jetlinks.supports.official;

import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class JetlinksTopicMessageCodecTest {

    JetlinksTopicMessageCodec codec = new JetlinksTopicMessageCodec();

    @Test
    public void testReadProperty() {

        ReadPropertyMessage readProperty = new ReadPropertyMessage();
        readProperty.setProperties(Arrays.asList("name"));
        readProperty.setMessageId("test");
        JetlinksTopicMessageCodec.EncodedTopic topic = codec.encode("test", readProperty);
        Assert.assertEquals(topic.getTopic(),"/test/properties/read");

    }

}