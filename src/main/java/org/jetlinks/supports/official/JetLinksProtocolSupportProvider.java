package org.jetlinks.supports.official;

import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.DeviceMessageCodec;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.supports.protocol.CompositeProtocolSupport;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

public class JetLinksProtocolSupportProvider implements ProtocolSupportProvider {

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
            JetLinksMQTTDeviceMessageCodec codec = new JetLinksMQTTDeviceMessageCodec();

            Supplier<Mono<DeviceMessageCodec>> codecSupplier = () -> Mono.just(codec);
            support.addMessageCodecSupport(DefaultTransport.MQTT, codecSupplier);
            support.addMessageCodecSupport(DefaultTransport.MQTTS, codecSupplier);

            return Mono.just(support);
        });
    }
}
