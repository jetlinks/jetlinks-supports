package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.defaults.DefaultDeviceOperator;
import org.jetlinks.core.defaults.DefaultDeviceProductOperator;
import org.jetlinks.core.device.*;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.supports.config.ClusterConfigStorageManager;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterDeviceRegistry implements DeviceRegistry {
    private DeviceMessageSenderInterceptor interceptor = new CompositeDeviceMessageSenderInterceptor();

    private ConfigStorageManager manager;

    private Cache<String, DeviceOperator> operatorCache;

    private Map<String, DeviceProductOperator> productOperatorMap = new ConcurrentHashMap<>();

    private ProtocolSupports supports;

    private DeviceOperationBroker handler;

    public ClusterDeviceRegistry(ProtocolSupports supports,
                                 ClusterManager clusterManager,
                                 DeviceOperationBroker handler) {
        this(supports, clusterManager, handler, CacheBuilder
                .newBuilder()
                .softValues()
                .expireAfterAccess(Duration.ofMinutes(30))
                .build());
    }

    public ClusterDeviceRegistry(ProtocolSupports supports,
                                 ClusterManager clusterManager,
                                 DeviceOperationBroker handler,
                                 Cache<String, DeviceOperator> cache) {
        this.supports = supports;
        this.handler = handler;
        this.manager = new ClusterConfigStorageManager(clusterManager);
        this.operatorCache = cache;
    }

    @Override
    public Flux<DeviceStateInfo> checkDeviceState(Flux<? extends Collection<String>> id) {

        return id.flatMap(list -> Flux.fromIterable(list)
                .flatMap(this::getDevice)
                .flatMap(device -> device
                        .getConnectionServerId()
                        .defaultIfEmpty("__")
                        .zipWith(Mono.just(device)))
                .groupBy(Tuple2::getT1, Tuple2::getT2)
                .flatMap(group -> {
                    if (!StringUtils.hasText(group.key()) || "__".equals(group.key())) {
                        return group.flatMap(operator -> operator
                                .getState()
                                .map(state -> new DeviceStateInfo(operator.getDeviceId(), state)));
                    }
                    return group
                            .map(DeviceOperator::getDeviceId)
                            .collectList()
                            .flatMapMany(deviceIdList -> handler.getDeviceState(group.key(), deviceIdList));
                }));
    }

    @Override
    public Mono<DeviceOperator> getDevice(String deviceId) {
        return Mono.justOrEmpty(operatorCache.getIfPresent(deviceId))
                .switchIfEmpty(Mono.defer(() -> {
                    DeviceOperator deviceOperator = createOperator(deviceId);
                    return deviceOperator.getConfig(DeviceConfigKey.protocol)
                            .doOnNext(r -> operatorCache.put(deviceId, deviceOperator))
                            .map((r) -> deviceOperator);
                }));
    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId) {
        return Mono.justOrEmpty(productOperatorMap.get(productId))
                .switchIfEmpty(Mono.defer(() -> {
                    DeviceProductOperator deviceOperator = createProductOperator(productId);
                    return deviceOperator.getConfig(DeviceConfigKey.protocol)
                            .doOnNext(r -> productOperatorMap.put(productId, deviceOperator))
                            .map((r) -> deviceOperator);
                }));
    }


    private DefaultDeviceOperator createOperator(String deviceId) {
        return new DefaultDeviceOperator(deviceId, supports, manager, handler, interceptor, this);
    }

    private DefaultDeviceProductOperator createProductOperator(String id) {
        return new DefaultDeviceProductOperator(id, supports, manager);
    }

    @Override
    public Mono<DeviceOperator> register(DeviceInfo deviceInfo) {
        return Mono.defer(() -> {
            DefaultDeviceOperator operator = new DefaultDeviceOperator(
                    deviceInfo.getId(),
                    supports, manager, handler, interceptor, this
            );
            operatorCache.put(operator.getDeviceId(), operator);

            Map<String, Object> configs = new HashMap<>();
            Optional.ofNullable(deviceInfo.getMetadata())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.metadata.getKey(), conf));
            Optional.ofNullable(deviceInfo.getProtocol())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.protocol.getKey(), conf));
            Optional.ofNullable(deviceInfo.getProductId())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.productId.getKey(), conf));

            Optional.ofNullable(deviceInfo.getConfiguration())
                    .ifPresent(configs::putAll);

            return operator.setConfigs(configs).thenReturn(operator);
        });
    }

    @Override
    public Mono<DeviceProductOperator> register(ProductInfo productInfo) {
        return Mono.defer(() -> {
            DefaultDeviceProductOperator operator = new DefaultDeviceProductOperator(productInfo.getId(), supports, manager);
            productOperatorMap.put(operator.getId(), operator);
            Map<String, Object> configs = new HashMap<>();
            Optional.ofNullable(productInfo.getMetadata())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.metadata.getKey(), conf));
            Optional.ofNullable(productInfo.getProtocol())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.protocol.getKey(), conf));

            Optional.ofNullable(productInfo.getConfiguration())
                    .ifPresent(configs::putAll);

            return operator.setConfigs(configs).thenReturn(operator);
        });
    }

    @Override
    public Mono<Void> unregisterDevice(String deviceId) {
        return manager.getStorage("device:" + deviceId)
                .flatMap(ConfigStorage::clear)
                .doOnSuccess(r -> operatorCache.invalidate(deviceId))
                .then();
    }

    @Override
    public Mono<Void> unregisterProduct(String productId) {
        return manager.getStorage("device-product:" + productId)
                .flatMap(ConfigStorage::clear)
                .doOnSuccess(r -> productOperatorMap.remove(productId))
                .then();
    }
}
