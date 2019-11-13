package org.jetlinks.supports.official;

import io.netty.buffer.Unpooled;
import org.jetlinks.core.defaults.CompositeProtocolSupports;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.ProductInfo;
import org.jetlinks.core.device.StandaloneDeviceMessageBroker;
import org.jetlinks.core.message.ChildDeviceMessage;
import org.jetlinks.core.message.ChildDeviceMessageReply;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.core.message.property.WritePropertyMessage;
import org.jetlinks.core.message.property.WritePropertyMessageReply;
import org.jetlinks.supports.server.session.TestDeviceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class JetLinksMqttDeviceMessageCodecTest {

    JetLinksMqttDeviceMessageCodec codec = new JetLinksMqttDeviceMessageCodec();


    TestDeviceRegistry registry;

    @Before
    public void init() {
        registry = new TestDeviceRegistry(new CompositeProtocolSupports(), new StandaloneDeviceMessageBroker());

        registry.registry(ProductInfo.builder()
                .id("product1")
                .protocol("jetlinks")
                .build())
                .flatMap(product -> registry.registry(DeviceInfo.builder()
                        .id("device1")
                        .productId("product1")
                        .build()))
                .subscribe();

    }

    @Test
    public void testReadProperty() {
        ReadPropertyMessage message = new ReadPropertyMessage();
        message.setDeviceId("device1");
        message.setMessageId("test");
        message.setProperties(Arrays.asList("name", "sn"));
        MqttMessage encodedMessage = codec.encode(createMessageContext(message)).block();


        Assert.assertNotNull(encodedMessage);
        Assert.assertEquals(encodedMessage.getTopic(), "/product1/device1/properties/read");
        System.out.println(encodedMessage.getPayload().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testReadChildProperty() {
        ReadPropertyMessage message = new ReadPropertyMessage();
        message.setDeviceId("test");
        message.setMessageId("test");
        message.setProperties(Arrays.asList("name", "sn"));
        ChildDeviceMessage childDeviceMessage = new ChildDeviceMessage();
        childDeviceMessage.setChildDeviceMessage(message);
        childDeviceMessage.setChildDeviceId("test");
        childDeviceMessage.setDeviceId("device1");

        MqttMessage encodedMessage = codec.encode(createMessageContext(childDeviceMessage)).block();


        Assert.assertNotNull(encodedMessage);
        Assert.assertEquals(encodedMessage.getTopic(), "/product1/device1/child/test/properties/read");
        System.out.println(encodedMessage.getPayload().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testReadPropertyReply() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/properties/read/reply")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"properties\":{\"sn\":\"test\"}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof ReadPropertyMessageReply);
        ReadPropertyMessageReply reply = ((ReadPropertyMessageReply) message);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "device1");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getProperties().get("sn"), "test");
        System.out.println(reply);
    }

    @Test
    public void testChildReadPropertyReply() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/child/test/properties/read/reply")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"properties\":{\"sn\":\"test\"}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof ChildDeviceMessageReply);
        ChildDeviceMessageReply childReply = ((ChildDeviceMessageReply) message);

        Assert.assertTrue(childReply.isSuccess());
        Assert.assertEquals(childReply.getDeviceId(),"device1");
        Assert.assertEquals(childReply.getMessageId(),"test");

        ReadPropertyMessageReply reply = (ReadPropertyMessageReply)childReply.getChildDeviceMessage();;
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "test");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getProperties().get("sn"), "test");
        System.out.println(reply);
    }

    @Test
    public void testWriteProperty() {
        WritePropertyMessage message = new WritePropertyMessage();
        message.setDeviceId("device1");
        message.setMessageId("test");
        message.setProperties(Collections.singletonMap("sn", "123"));
        MqttMessage encodedMessage = codec.encode(createMessageContext(message)).block();


        Assert.assertNotNull(encodedMessage);
        Assert.assertEquals(encodedMessage.getTopic(), "/product1/device1/properties/write");
        System.out.println(encodedMessage.getPayload().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testChildWriteProperty() {
        WritePropertyMessage message = new WritePropertyMessage();
        message.setDeviceId("device1");
        message.setMessageId("test");
        message.setProperties(Collections.singletonMap("sn", "123"));

        ChildDeviceMessage childDeviceMessage = new ChildDeviceMessage();
        childDeviceMessage.setChildDeviceMessage(message);
        childDeviceMessage.setChildDeviceId("test");
        childDeviceMessage.setDeviceId("device1");


        MqttMessage encodedMessage = codec.encode(createMessageContext(childDeviceMessage)).block();


        Assert.assertNotNull(encodedMessage);
        Assert.assertEquals(encodedMessage.getTopic(), "/product1/device1/child/test/properties/write");
        System.out.println(encodedMessage.getPayload().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testWritePropertyReply() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/properties/write/reply")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"properties\":{\"sn\":\"test\"}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof WritePropertyMessageReply);
        WritePropertyMessageReply reply = ((WritePropertyMessageReply) message);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "device1");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getProperties().get("sn"), "test");
        System.out.println(reply);
    }



    @Test
    public void testWriteChildPropertyReply() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/child/test/properties/write/reply")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"properties\":{\"sn\":\"test\"}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof ChildDeviceMessageReply);
        ChildDeviceMessageReply childReply = ((ChildDeviceMessageReply) message);

        Assert.assertTrue(childReply.isSuccess());
        Assert.assertEquals(childReply.getDeviceId(),"device1");
        Assert.assertEquals(childReply.getMessageId(),"test");

        WritePropertyMessageReply reply = (WritePropertyMessageReply)childReply.getChildDeviceMessage();;
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "test");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getProperties().get("sn"), "test");
        System.out.println(reply);
    }


    @Test
    public void testInvokeFunction() {
        FunctionInvokeMessage message = new FunctionInvokeMessage();
        message.setDeviceId("device1");
        message.setMessageId("test");
        message.setFunctionId("playVoice");
        message.addInput("file", "http://baidu.com/1.mp3");

        MqttMessage encodedMessage = codec.encode(createMessageContext(message)).block();


        Assert.assertNotNull(encodedMessage);
        Assert.assertEquals(encodedMessage.getTopic(), "/product1/device1/function/invoke");
        System.out.println(encodedMessage.getPayload().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testInvokeFunctionReply() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/function/invoke/reply")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"output\":\"ok\"}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof FunctionInvokeMessageReply);
        FunctionInvokeMessageReply reply = ((FunctionInvokeMessageReply) message);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "device1");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getOutput(), "ok");
        System.out.println(reply);
    }

    @Test
    public void testInvokeChildFunctionReply() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/child/test/function/invoke/reply")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"output\":\"ok\"}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof ChildDeviceMessageReply);
        ChildDeviceMessageReply childReply = ((ChildDeviceMessageReply) message);

        Assert.assertTrue(childReply.isSuccess());
        Assert.assertEquals(childReply.getDeviceId(),"device1");
        Assert.assertEquals(childReply.getMessageId(),"test");

        FunctionInvokeMessageReply reply = (FunctionInvokeMessageReply)childReply.getChildDeviceMessage();;
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "test");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getOutput(), "ok");
        System.out.println(reply);
    }

    @Test
    public void testEvent() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/event/temp")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"data\":100}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof EventMessage);
        EventMessage reply = ((EventMessage) message);
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "device1");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getData(), 100);
        System.out.println(reply);
    }

    @Test
    public void testChildEvent() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/child/test/event/temp")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"data\":100}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof ChildDeviceMessageReply);

        EventMessage reply = ((EventMessage) ((ChildDeviceMessageReply) message).getChildDeviceMessage());
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "test");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getData(), 100);
        System.out.println(reply);
    }

    @Test
    public void testPropertiesReport() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/properties/report")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"data\":{\"sn\":\"test\"}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof EventMessage);
        EventMessage reply = ((EventMessage) message);
        Assert.assertTrue(reply.getHeader(Headers.reportProperties).orElse(false));
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "device1");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getData(), Collections.singletonMap("sn", "test"));
        System.out.println(reply);
    }


    @Test
    public void testChildPropertiesReport() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/child/test/properties/report")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"data\":{\"sn\":\"test\"}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof ChildDeviceMessageReply);

        EventMessage reply = ((EventMessage) ((ChildDeviceMessageReply) message).getChildDeviceMessage());
        Assert.assertTrue(reply.getHeader(Headers.reportProperties).orElse(false));
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "test");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getData(), Collections.singletonMap("sn", "test"));
        System.out.println(reply);
    }


    @Test
    public void testMetadataDerived() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/metadata/derived")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"data\":{\"functions\":[]}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof EventMessage);
        EventMessage reply = ((EventMessage) message);
        Assert.assertTrue(reply.getHeader(Headers.reportDerivedMetadata).orElse(false));
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "device1");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getData(), Collections.singletonMap("functions", Collections.emptyList()));
        System.out.println(reply);
    }

    @Test
    public void testChildMetadataDerived() {
        Message message = codec.decode(createMessageContext(SimpleMqttMessage.builder()
                .topic("/product1/device1/child/test/metadata/derived")
                .payload(Unpooled.copiedBuffer("{\"messageId\":\"test\",\"data\":{\"functions\":[]}}".getBytes()))
                .build())).block();

        Assert.assertTrue(message instanceof ChildDeviceMessageReply);

        EventMessage reply = ((EventMessage) ((ChildDeviceMessageReply) message).getChildDeviceMessage());
        Assert.assertTrue(reply.getHeader(Headers.reportDerivedMetadata).orElse(false));
        Assert.assertTrue(reply.isSuccess());
        Assert.assertEquals(reply.getDeviceId(), "test");
        Assert.assertEquals(reply.getMessageId(), "test");
        Assert.assertEquals(reply.getData(), Collections.singletonMap("functions", Collections.emptyList()));
        System.out.println(reply);
    }

    public MessageEncodeContext createMessageContext(Message message) {

        return new MessageEncodeContext() {
            @Override
            public Message getMessage() {
                return message;
            }

            @Override
            public DeviceOperator getDevice() {
                return registry.getDevice("device1").block();
            }
        };
    }


    public MessageDecodeContext createMessageContext(EncodedMessage message) {

        return new MessageDecodeContext() {
            @Override
            public EncodedMessage getMessage() {
                return message;
            }

            @Override
            public DeviceOperator getDevice() {
                return registry.getDevice("device1").block();
            }
        };
    }


}