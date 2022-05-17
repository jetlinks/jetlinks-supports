package org.jetlinks.supports.scalecube;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.AllArgsConstructor;
import org.jetlinks.core.cluster.load.LoadSupplier;
import org.jetlinks.core.cluster.load.LoadBalancer;
import org.jetlinks.core.cluster.load.ServerLoad;
import org.jetlinks.core.rpc.RpcManager;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
public class ClusterLoadBalancer implements LoadBalancer {

    private final RpcManager rpcManager;

    private final Map<String, LoadSupplier> suppliers = new ConcurrentHashMap<>();

    private final Disposable.Composite disposable = Disposables.composite();

    public void init() {
        disposable.add(rpcManager.registerService(new LoadBalancerServiceImpl(this)));
    }

    @Override
    public Disposable register(LoadSupplier loadSupplier) {
        suppliers.put(loadSupplier.loadId(), loadSupplier);
        loadSupplier.init(this);
        return () -> suppliers.remove(loadSupplier.loadId(), loadSupplier);
    }

    @Override
    public Flux<ServerLoad> loads() {
        return rpcManager
                .getServices(LoadBalancerService.class)
                .flatMap(service -> service.service().loads());
    }

    @Override
    public Mono<ServerLoad> load(String serviceNodeId, String featureId) {
        return rpcManager
                .getService(serviceNodeId, LoadBalancerService.class)
                .flatMap(service -> service.load(featureId));
    }

    @Override
    public Flux<ServerLoad> loads(String serviceNodeId) {
        return rpcManager
                .getService(serviceNodeId, LoadBalancerService.class)
                .flatMapMany(LoadBalancerService::loads);
    }

    @Override
    public Flux<ServerLoad> localLoads() {

        return Flux.fromIterable(suppliers.values())
                   .flatMap(this::getLoad);
    }

    @Override
    public Mono<ServerLoad> localLoad(String featureId) {
        LoadSupplier supplier = suppliers.get(featureId);
        if (null != supplier) {
            return getLoad(supplier);
        }
        return Mono.empty();
    }

    private Mono<ServerLoad> getLoad(LoadSupplier supplier) {
        return supplier
                .currentLoad()
                .map(load -> ServerLoad.of(currentServiceId(), supplier.loadId(), load));
    }

    public String currentServiceId() {
        return rpcManager.currentServerId();
    }

    @Service
    public interface LoadBalancerService {

        @ServiceMethod
        Flux<ServerLoad> loads();

        @ServiceMethod
        Mono<ServerLoad> load(String featureId);

    }

    @AllArgsConstructor
    static class LoadBalancerServiceImpl implements LoadBalancerService {
        private final LoadBalancer loadBalancer;

        @Override
        public Flux<ServerLoad> loads() {
            return loadBalancer.localLoads();
        }

        @Override
        public Mono<ServerLoad> load(String featureId) {
            return loadBalancer.localLoad(featureId);
        }
    }
}
