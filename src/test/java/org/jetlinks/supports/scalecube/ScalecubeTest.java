package org.jetlinks.supports.scalecube;

import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.junit.Test;
import reactor.core.publisher.Mono;

public class ScalecubeTest {


    @Test
    @SneakyThrows
    public void test() {
        Microservices seed;

        {

            seed = Microservices
                    .builder()
                    .discovery(serviceEndpoint -> new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new TcpTransportFactory()))
                            .options(cfg->cfg.metadata(serviceEndpoint))
                    )
                    .transport(RSocketServiceTransport::new)
                    .services(new TestApiImpl())
                    .startAwait();

            seed.listenDiscovery()
                .subscribe(event -> {
                    System.out.println(event.serviceEndpoint());
                });
        }
        Microservices ms;
        {
            ms = Microservices
                    .builder()
                    .discovery(serviceEndpoint -> new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new TcpTransportFactory()))
                            .membership(conf -> conf.seedMembers(seed.discovery().address()))
                            .options(cfg-> cfg.metadata(serviceEndpoint))
                    )
                    .transport(RSocketServiceTransport::new)
                    .startAwait();
        }

        // Create service proxy
        TestApi service = ms
                .call()
                .api(TestApi.class);

        // Execute the services and subscribe to service events
        System.out.println(service.lowercase(1L).block());
        System.out.println(service.add(new Long[]{1L, 1L}).block());

        Mono.whenDelayError(seed.shutdown(), ms.shutdown()).block();
    }
}
