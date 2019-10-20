package org.jetlinks.supports.official;

import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.supports.CompositeProtocolSupport;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import reactor.core.publisher.Mono;

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

            JetLinksMQTTDeviceMessageCodec codec = new JetLinksMQTTDeviceMessageCodec();

            support.addMessageCodecSupport(DefaultTransport.MQTT, () -> Mono.just(codec));
            support.addMessageCodecSupport(DefaultTransport.MQTTS, () -> Mono.just(codec));

            return Mono.just(support);
        });
    }
}
