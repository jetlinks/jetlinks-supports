package org.jetlinks.supports.scalecube;

import io.scalecube.net.Address;
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

        Microservices seed =
                Microservices.builder()
                             .discovery(
                                     serviceEndpoint ->
                                             new ScalecubeServiceDiscovery()
                                                     .transport(cfg -> cfg.transportFactory(new TcpTransportFactory()))
                                                     .options(opts -> opts.metadata(serviceEndpoint)))
                             .transport(RSocketServiceTransport::new)
                             .startAwait();


        seed.listenDiscovery()
            .subscribe(event->{
                System.out.println(event.serviceEndpoint());
            });

        final Address seedAddress = seed.discovery().address();

        // Construct a ScaleCube node which joins the cluster hosting the Greeting Service
        Microservices ms =
                Microservices.builder()
                             .discovery(
                                     "ms",
                                     endpoint ->
                                             new ScalecubeServiceDiscovery()
                                                     .transport(cfg -> cfg.transportFactory(new TcpTransportFactory()))
                                                     .options(opts -> opts.metadata(endpoint))
                                                     .membership(cfg -> cfg.seedMembers(seedAddress)))
                             .transport(RSocketServiceTransport::new)
                             .services(new TestApiImpl())
                             .startAwait();


        // Create service proxy
        TestApi service = seed
                .call()
                .api(TestApi.class);

        // Execute the services and subscribe to service events
        System.out.println(service.lowercase(1L).block());

        Mono.whenDelayError(seed.shutdown(), ms.shutdown()).block();
    }
}
