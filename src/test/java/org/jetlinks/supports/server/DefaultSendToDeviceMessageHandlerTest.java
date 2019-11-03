package org.jetlinks.supports.server;

import lombok.SneakyThrows;
import org.jetlinks.core.defaults.CompositeProtocolSupports;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.StandaloneDeviceMessageBroker;
import org.jetlinks.core.message.ChildDeviceMessage;
import org.jetlinks.core.message.ChildDeviceMessageReply;
import org.jetlinks.core.message.property.ReadPropertyMessageReply;
import org.jetlinks.core.server.monitor.GatewayServerMetrics;
import org.jetlinks.core.server.monitor.GatewayServerMonitor;
import org.jetlinks.supports.TestDeviceSession;
import org.jetlinks.supports.protocol.ServiceLoaderProtocolSupports;
import org.jetlinks.supports.server.monitor.MicrometerGatewayServerMetrics;
import org.jetlinks.supports.server.session.DefaultDeviceSessionManager;
import org.jetlinks.supports.server.session.TestDeviceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.test.StepVerifier;

import static org.junit.Assert.*;

public class DefaultSendToDeviceMessageHandlerTest {
    private StandaloneDeviceMessageBroker broker;

    private DefaultDeviceSessionManager sessionManager;

    private TestDeviceRegistry registry;

    private DefaultSendToDeviceMessageHandler handler;

    private DefaultDecodedClientMessageHandler decodedHandler;

    @Before
    public void init() {
        sessionManager = new DefaultDeviceSessionManager();
        sessionManager.setGatewayServerMonitor(new GatewayServerMonitor() {
            @Override
            public String getCurrentServerId() {
                return "test";
            }

            @Override
            public GatewayServerMetrics metrics() {
                return new MicrometerGatewayServerMetrics(getCurrentServerId());
            }
        });
        broker = new StandaloneDeviceMessageBroker();

        ServiceLoaderProtocolSupports supports = new ServiceLoaderProtocolSupports();
        supports.init();
        registry = new TestDeviceRegistry(supports, broker);

        sessionManager.setRegistry(registry);

        decodedHandler = new DefaultDecodedClientMessageHandler(broker, sessionManager);

        handler = new DefaultSendToDeviceMessageHandler("test", sessionManager, broker, registry);
        sessionManager.init();
        handler.startup();
    }

    @Test
    @SneakyThrows
    public void testMessage() {
        DeviceOperator device = registry.registry(DeviceInfo.builder()
                .id("test")
                .protocol("jetlinks.v1.0")
                .build())
                .block();

        DeviceOperator children = registry.registry(DeviceInfo.builder()
                .id("test-children")
                .protocol("jetlinks.v1.0")
                .build())
                .block();

        children.setConfig(DeviceConfigKey.parentMeshDeviceId, "test").block();

        sessionManager.register(new TestDeviceSession("test", device, message -> {

            ReadPropertyMessageReply readPropertyMessageReply = new ReadPropertyMessageReply();
            readPropertyMessageReply.setMessageId("test");
            readPropertyMessageReply.setSuccess(true);

            ChildDeviceMessageReply reply = new ChildDeviceMessageReply();
            reply.setMessageId(readPropertyMessageReply.getMessageId());
            reply.setChildDeviceId("test-children");
            reply.setChildDeviceMessage(readPropertyMessageReply);

            decodedHandler.handleMessage(sessionManager.getSession("test"), reply)
                    .subscribe();
        }));

        sessionManager.registerChildren("test", "test-children").block();


        Thread.sleep(100);
        children.messageSender()
                .readProperty("name")
                .messageId("test")
                .send()
                .map(ReadPropertyMessageReply::isSuccess)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();


    }

}