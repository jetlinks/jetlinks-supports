package org.jetlinks.supports.device;

import com.alibaba.fastjson.JSONObject;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.device.DeviceModule;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.module.DeviceModuleMessage;
import org.jetlinks.core.message.property.ReadPropertyMessage;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.SimplePropertyMetadata;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.core.things.ThingMetadata;
import org.jetlinks.supports.config.InMemoryConfigStorageManager;
import org.jetlinks.supports.official.DefaultThingsMetadata;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultDeviceModuleThingProviderTest {

    @Test
    public void testCreateModuleByParentMetadataAndInstanceStorage() {
        DeviceOperator device = createDevice();
        DefaultDeviceModuleThingProvider provider = new DefaultDeviceModuleThingProvider(new InMemoryConfigStorageManager());

        StepVerifier.create(provider.getModuleThing(device, "board", "slot-1"))
                    .assertNext(module -> {
                        Assert.assertEquals("device-1", module.getDeviceId());
                        Assert.assertEquals("board", module.getCode());
                        Assert.assertEquals("slot-1", module.getInstanceCode());
                        Assert.assertEquals("device-1:slot-1", module.getId());
                    })
                    .verifyComplete();
    }

    @Test
    public void testModuleConfigUsesInstanceCacheAndFallbackParent() {
        DeviceOperator device = createDevice();
        when(device.getConfig("parentOnly")).thenReturn(Mono.just(Value.simple("parent")));
        when(device.getConfigs(java.util.Set.of("parentOnly", "selfOnly")))
            .thenReturn(Mono.just(Values.of(Map.of("parentOnly", "parent"))));

        DefaultDeviceModuleThingProvider provider = new DefaultDeviceModuleThingProvider(new InMemoryConfigStorageManager());
        DeviceModule module = provider.getModuleThing(device, "board", "slot-1").block();
        Assert.assertNotNull(module);

        StepVerifier.create(module.setConfig("selfOnly", "self"))
                    .expectNext(true)
                    .verifyComplete();
        StepVerifier.create(module.getSelfConfig("selfOnly").map(Value::asString))
                    .expectNext("self")
                    .verifyComplete();
        StepVerifier.create(module.getConfig("parentOnly").map(Value::asString))
                    .expectNext("parent")
                    .verifyComplete();
        StepVerifier.create(module.getConfigs(java.util.Set.of("parentOnly", "selfOnly")))
                    .assertNext(values -> {
                        Assert.assertEquals("parent", values.getValue("parentOnly").get().asString());
                        Assert.assertEquals("self", values.getValue("selfOnly").get().asString());
                    })
                    .verifyComplete();
    }

    @Test
    public void testRpcWrapsModuleContextAndRouteKey() {
        DeviceOperator device = createDevice();
        when(device.rpc()).thenReturn(message -> Flux.just(message));
        DefaultDeviceModuleThingProvider provider = new DefaultDeviceModuleThingProvider(new InMemoryConfigStorageManager());
        DeviceModule module = provider.getModuleThing(device, "board", "slot-1").block();
        Assert.assertNotNull(module);

        ReadPropertyMessage message = new ReadPropertyMessage();
        message.setDeviceId("device-1");
        message.setMessageId("msg-1");
        message.addHeader("module", "bad");
        message.addHeader("moduleInstance", "bad");

        StepVerifier.create(module.rpc().call(message))
                    .assertNext(wrapper -> {
                        Assert.assertTrue(wrapper instanceof DeviceModuleMessage);
                        DeviceModuleMessage moduleMessage = (DeviceModuleMessage) wrapper;
                        Assert.assertEquals("device-1", moduleMessage.getDeviceId());
                        Assert.assertEquals("board", moduleMessage.getModule());
                        Assert.assertEquals("slot-1", moduleMessage.getModuleInstance());
                        Assert.assertEquals("device-1", moduleMessage.getHeaderOrElse(Headers.routeKey, null));
                        Assert.assertEquals("board", moduleMessage.getHeaderOrElse("module", null));
                        Assert.assertEquals("slot-1", moduleMessage.getHeaderOrElse("moduleInstance", null));
                    })
                    .verifyComplete();
    }


    @Test
    public void testCreateModulesFromDeviceConfig() {
        DeviceOperator device = createDevice();
        when(device.getConfig(DefaultDeviceModuleThingProvider.MODULES_CONFIG_KEY))
            .thenReturn(Mono.just(List.of(
                moduleInfo("module-id-1", "board", "slot-1"),
                moduleInfo("module-id-2", "board", "slot-2"),
                moduleInfo("module-id-3", "network", "eth0")
            )));
        DefaultDeviceModuleThingProvider provider = new DefaultDeviceModuleThingProvider(new InMemoryConfigStorageManager());

        StepVerifier.create(provider.getModuleThings(device, "board").map(DeviceModule::getInstanceCode))
                    .expectNext("slot-1", "slot-2")
                    .verifyComplete();
        StepVerifier.create(provider.getModuleThing(device, "board", "slot-2"))
                    .assertNext(module -> {
                        Assert.assertEquals("slot-2", module.getInstanceCode());
                        Assert.assertEquals("module-id-2", module.getId());
                    })
                    .verifyComplete();
    }

    @Test
    public void testMissingModuleDefinitionReturnsEmpty() {
        DeviceOperator device = createDevice();
        DefaultDeviceModuleThingProvider provider = new DefaultDeviceModuleThingProvider(new InMemoryConfigStorageManager());

        StepVerifier.create(provider.getModuleThing(device, "missing", "missing-1"))
                    .verifyComplete();
    }


    @Test
    public void testUpdateMetadataStoresParsedObjectAndMergesOnDemand() {
        DeviceOperator device = createDevice();
        DefaultDeviceModuleThingProvider provider = new DefaultDeviceModuleThingProvider(new InMemoryConfigStorageManager());
        DeviceModule module = provider.getModuleThing(device, "board", "slot-1").block();
        Assert.assertNotNull(module);

        String customMetadata = "{\"id\":\"board\",\"properties\":[{\"id\":\"custom\",\"name\":\"自定义\",\"valueType\":{\"type\":\"string\"}}]}";
        StepVerifier.create(module.updateMetadata(customMetadata))
                    .expectNext(true)
                    .verifyComplete();

        StepVerifier.create(module.getSelfConfig(DefaultDeviceModule.METADATA_KEY).map(Value::get))
                    .assertNext(value -> Assert.assertTrue(value instanceof JSONObject))
                    .verifyComplete();
        StepVerifier.create(module.getMetadata())
                    .assertNext(metadata -> Assert.assertNotNull(metadata.getPropertyOrNull("custom")))
                    .verifyComplete();
    }

    @Test
    public void testModuleConfigurationMetadataSupportsThingMetadataObject() {
        DeviceOperator device = createDevice();
        DefaultThingsMetadata custom = new DefaultThingsMetadata("board", "控制板");
        custom.addProperty(SimplePropertyMetadata.of("custom", "自定义", StringType.GLOBAL));
        DeviceModuleInfo info = moduleInfo("module-id-1", "board", "slot-1");
        info.setConfiguration(Map.of(DefaultDeviceModule.METADATA_KEY, custom));

        when(device.getConfig(DefaultDeviceModuleThingProvider.MODULES_CONFIG_KEY))
            .thenReturn(Mono.just(List.of(info)));

        DefaultDeviceModuleThingProvider provider = new DefaultDeviceModuleThingProvider(new InMemoryConfigStorageManager());

        StepVerifier.create(provider.getModuleThing(device, "board", "slot-1")
                                    .flatMap(DeviceModule::getMetadata))
                    .assertNext(metadata -> Assert.assertNotNull(metadata.getPropertyOrNull("custom")))
                    .verifyComplete();
    }


    private DeviceModuleInfo moduleInfo(String id, String code, String instanceCode) {
        DeviceModuleInfo info = new DeviceModuleInfo();
        info.setId(id);
        info.setDeviceId("device-1");
        info.setCode(code);
        info.setInstanceCode(instanceCode);
        return info;
    }

    private DeviceOperator createDevice() {
        ThingMetadata module = new DefaultThingsMetadata("board", "控制板");
        DeviceMetadata metadata = mock(DeviceMetadata.class);
        when(metadata.getModule("board")).thenReturn(Optional.of(module));
        when(metadata.getModule("missing")).thenReturn(Optional.empty());

        DeviceOperator device = mock(DeviceOperator.class);
        when(device.getDeviceId()).thenReturn("device-1");
        when(device.getMetadata()).thenReturn(Mono.just(metadata));
        when(device.getConfig("parentOnly")).thenReturn(Mono.empty());
        when(device.getConfig(DefaultDeviceModuleThingProvider.MODULES_CONFIG_KEY)).thenReturn(Mono.empty());
        when(device.getConfigs(java.util.Set.of("parentOnly", "selfOnly"))).thenReturn(Mono.just(Values.of(Map.of())));
        return device;
    }
}
