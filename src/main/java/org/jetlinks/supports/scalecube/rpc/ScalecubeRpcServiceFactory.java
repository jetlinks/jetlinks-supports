package org.jetlinks.supports.scalecube.rpc;

import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.ServiceTransport;
import lombok.AllArgsConstructor;
import org.jetlinks.core.rpc.DisposableService;
import org.jetlinks.core.rpc.RpcServiceFactory;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import reactor.core.Disposable;

import java.util.function.Supplier;

@AllArgsConstructor
public class ScalecubeRpcServiceFactory implements RpcServiceFactory {
    private final ExtendedCluster cluster;

    private final Supplier<ServiceTransport> transportSupplier;

    private final ServiceMethodRegistry registry;


    public void init() {

    }

    @Override
    public <T> DisposableService<T> createConsumer(String address, Class<T> serviceInterface) {

        return null;
    }

    @Override
    public <T> Disposable createProducer(String address, Class<T> serviceInterface, T instance) {


        return () -> {

        };
    }

}
