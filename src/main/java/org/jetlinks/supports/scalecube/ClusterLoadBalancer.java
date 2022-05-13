package org.jetlinks.supports.scalecube;

import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.AllArgsConstructor;
import org.jetlinks.core.cluster.load.FeatureLoadSupplier;
import org.jetlinks.core.cluster.load.LoadBalancer;
import org.jetlinks.core.cluster.load.ServerLoad;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@AllArgsConstructor
public class ClusterLoadBalancer implements LoadBalancer {

    private final ExtendedCluster cluster;

    private final ServiceCall serviceCall;

    private final Map<String, FeatureLoadSupplier> suppliers = new ConcurrentHashMap<>();

    private final Map<String, LoadBalancerService> services = new ConcurrentHashMap<>();


    private final Disposable.Composite disposable = Disposables.composite();


    public void init() {

        cluster.handler(ignore -> new ClusterLoadBalancerHandler());


    }


    class ClusterLoadBalancerHandler implements ClusterMessageHandler {
        @Override
        public void onMessage(Message message) {
            //注册Service

        }

        @Override
        public void onGossip(Message gossip) {
            onMessage(gossip);
        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {

        }
    }

    @Override
    public Disposable register(FeatureLoadSupplier loadSupplier) {
        suppliers.put(loadSupplier.featureId(), loadSupplier);
        return () -> suppliers.remove(loadSupplier.featureId(), loadSupplier);
    }

    @Override
    public Flux<ServerLoad> loads() {
        return Flux.fromIterable(services.values())
                   .flatMap(LoadBalancerService::loads);
    }

    @Override
    public Mono<ServerLoad> load(String serviceNodeId, String featureId) {
        LoadBalancerService service = services.get(serviceNodeId);
        if (service == null) {
            return Mono.empty();
        }
        return service.load(featureId);
    }

    @Override
    public Flux<ServerLoad> loads(String serviceNodeId) {
        LoadBalancerService service = services.get(serviceNodeId);
        if (service == null) {
            return Flux.empty();
        }
        return service.loads();
    }

    @Override
    public Flux<ServerLoad> localLoads() {

        return Flux.fromIterable(suppliers.values())
                   .flatMap(this::getLoad);
    }

    @Override
    public Mono<ServerLoad> localLoad(String featureId) {
        FeatureLoadSupplier supplier = suppliers.get(featureId);
        if (null != supplier) {
            return getLoad(supplier);
        }
        return Mono.empty();
    }

    private Mono<ServerLoad> getLoad(FeatureLoadSupplier supplier) {
        return supplier
                .currentLoad()
                .map(load -> ServerLoad.of(currentServiceId(), supplier.featureId(), load));
    }

    public String currentServiceId() {
        return cluster.member().alias() == null ? cluster.member().id() : cluster.member().alias();
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
        private final Supplier<LoadBalancer> loadBalancer;

        @Override
        public Flux<ServerLoad> loads() {
            return loadBalancer
                    .get()
                    .localLoads();
        }

        @Override
        public Mono<ServerLoad> load(String featureId) {
            return loadBalancer
                    .get()
                    .localLoad(featureId);
        }
    }
}
