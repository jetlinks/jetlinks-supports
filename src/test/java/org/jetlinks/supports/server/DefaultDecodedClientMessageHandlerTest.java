package org.jetlinks.supports.server;

import org.jetlinks.core.defaults.CompositeProtocolSupports;
import org.jetlinks.core.device.*;
import org.jetlinks.core.message.*;
import org.jetlinks.core.server.monitor.GatewayServerMetrics;
import org.jetlinks.core.server.monitor.GatewayServerMonitor;
import org.jetlinks.core.server.session.ChildrenDeviceSession;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.supports.TestDeviceSession;
import org.jetlinks.supports.server.monitor.MicrometerGatewayServerMetrics;
import org.jetlinks.supports.server.session.DefaultDeviceSessionManager;
import org.jetlinks.supports.server.session.TestDeviceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.test.StepVerifier;

public class DefaultDecodedClientMessageHandlerTest {


    private DefaultDecodedClientMessageHandler handler;

    private StandaloneDeviceMessageBroker broker;

    private DefaultDeviceSessionManager sessionManager;

    private TestDeviceRegistry registry;

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
        registry = new TestDeviceRegistry(new CompositeProtocolSupports(), broker);
        sessionManager.setRegistry(registry);
        broker = new StandaloneDeviceMessageBroker();
        handler = new DefaultDecodedClientMessageHandler(
                broker,
                sessionManager
        );

        DeviceOperator device = registry.register(DeviceInfo.builder()
                .id("test")
                .protocol("jetlinks-v1.0")
                .build())
                .block();

        DeviceOperator children = registry.register(DeviceInfo.builder()
                .id("test-children")
                .protocol("jetlinks-v1.0")
                .build())
                .block();

        children.setConfig(DeviceConfigKey.parentGatewayId, "test").block();

        sessionManager.register(new TestDeviceSession("test", device, message -> {

        }));


    }

    @Test
    public void testOnlineOfflineChildrenReplyMessage() {

        DeviceSession session = sessionManager.getSession("test");
        Assert.assertNotNull(session);

        ChildDeviceMessageReply reply=new ChildDeviceMessageReply();
        reply.setChildDeviceId("test-children");


        DeviceOnlineMessage onlineMessage = new DeviceOnlineMessage();
        onlineMessage.setDeviceId("test-children");

        reply.setChildDeviceMessage(onlineMessage);

        handler.handleMessage(session.getOperator(), reply)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        ChildrenDeviceSession deviceSession = sessionManager.getSession("test", "test-children");
        registry.getDevice("test-children")
                .flatMap(DeviceOperator::getState)
                .as(StepVerifier::create)
                .expectNext(DeviceState.online)
                .verifyComplete();
        Assert.assertNotNull(deviceSession);

        DeviceOfflineMessage offlineMessage = new DeviceOfflineMessage();
        offlineMessage.setDeviceId("test-children");

        reply.setChildDeviceMessage(offlineMessage);

        handler.handleMessage(session.getOperator(), reply)
                .as(StepVerifier::create)
                .expectNext(true)
                .verifyComplete();

        deviceSession = sessionManager.getSession("test", "test-children");

        Assert.assertNull(deviceSession);

        registry.getDevice("test-children")
                .flatMap(DeviceOperator::getState)
                .as(StepVerifier::create)
                .expectNext(DeviceState.offline)
                .verifyComplete();

    }

}
