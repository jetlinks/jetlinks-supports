package org.jetlinks.supports.official;

import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.DeviceMessageCodec;
import org.jetlinks.core.metadata.ConfigMetadata;
import org.jetlinks.core.metadata.DefaultConfigMetadata;
import org.jetlinks.core.metadata.types.PasswordType;
import org.jetlinks.core.metadata.types.StringType;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.core.defaults.CompositeProtocolSupport;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

public class JetLinksProtocolSupportProvider implements ProtocolSupportProvider {

    private static final DefaultConfigMetadata mqttConfig = new DefaultConfigMetadata(
            "MQTT认证配置"
            , "MQTT认证时需要的配置,mqtt用户名,密码算法:\n" +
            "username=secureId|timestamp\n" +
            "password=md5(secureId|timestamp|secureKey)\n" +
            "\n" +
            "timestamp为时间戳,与服务时间不能相差5分钟")
            .add("secureId", "secureId", "密钥ID", new StringType())
            .add("secureKey", "secureKey", "密钥KEY", new PasswordType());

    @Override
    public Mono<CompositeProtocolSupport> create(ServiceContext context) {

        return Mono.defer(() -> {
            CompositeProtocolSupport support = new CompositeProtocolSupport();

            support.setId("jetlinks.v1.0");
            support.setName("JetLinks V1.0");
            support.setDescription("JetLinks Protocol Version 1.0");

            support.addAuthenticator(DefaultTransport.MQTT, new JetLinksAuthenticator());
            support.addAuthenticator(DefaultTransport.MQTTS, new JetLinksAuthenticator());

            support.setMetadataCodec(new JetLinksDeviceMetadataCodec());
            Supplier<Mono<ConfigMetadata>> mqttConfigSupplier = () -> Mono.just(mqttConfig);

            support.addConfigMetadata(DefaultTransport.MQTT, mqttConfigSupplier);
            support.addConfigMetadata(DefaultTransport.MQTTS, mqttConfigSupplier);

            JetLinksMqttDeviceMessageCodec codec = new JetLinksMqttDeviceMessageCodec();

            Supplier<Mono<DeviceMessageCodec>> codecSupplier = () -> Mono.just(codec);
            support.addMessageCodecSupport(DefaultTransport.MQTT, codecSupplier);
            support.addMessageCodecSupport(DefaultTransport.MQTTS, codecSupplier);

            return Mono.just(support);
        });
    }
}
