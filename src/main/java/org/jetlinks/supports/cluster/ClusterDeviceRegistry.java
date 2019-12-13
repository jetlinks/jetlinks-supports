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
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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
    public Mono<DeviceOperator> getDevice(String deviceId) {
        return Mono.justOrEmpty(operatorCache.getIfPresent(deviceId))
                .switchIfEmpty(Mono.defer(() -> {
                    DeviceOperator deviceOperator = createOperator(deviceId);
                    return deviceOperator.getConfig(DeviceConfigKey.protocol)
                            .doOnNext(r -> operatorCache.put(deviceId, deviceOperator))
                            .thenReturn(deviceOperator);
                }));
    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId) {
        return Mono.justOrEmpty(productOperatorMap.get(productId))
                .switchIfEmpty(Mono.defer(() -> {
                    DeviceProductOperator deviceOperator = createProductOperator(productId);
                    return deviceOperator.getConfig(DeviceConfigKey.protocol)
                            .doOnNext(r -> productOperatorMap.put(productId, deviceOperator))
                            .thenReturn(deviceOperator);
                }));
    }


    private DefaultDeviceOperator createOperator(String deviceId) {
        return new DefaultDeviceOperator(deviceId, supports, manager, handler, interceptor, this);
    }

    private DefaultDeviceProductOperator createProductOperator(String id) {
        return new DefaultDeviceProductOperator(id, supports, manager);
    }

    @Override
    public Mono<DeviceOperator> registry(DeviceInfo deviceInfo) {
        DefaultDeviceOperator operator = createOperator(deviceInfo.getId());
        operatorCache.put(operator.getDeviceId(), operator);
        Map<String, Object> configs = new HashMap<>();

        configs.put(DeviceConfigKey.productId.getKey(), deviceInfo.getProductId());
        configs.put(DeviceConfigKey.protocol.getKey(), deviceInfo.getProtocol());

        return operator.setConfigs(
                DeviceConfigKey.productId.value(deviceInfo.getProductId()),
                DeviceConfigKey.protocol.value(deviceInfo.getProtocol()))
                .thenReturn(operator);
    }

    @Override
    public Mono<DeviceProductOperator> registry(ProductInfo productInfo) {
        DefaultDeviceProductOperator operator = new DefaultDeviceProductOperator(productInfo.getId(), supports, manager);
        productOperatorMap.put(operator.getId(), operator);
        return operator.setConfigs(
                DeviceConfigKey.metadata.value(productInfo.getMetadata()),
                DeviceConfigKey.protocol.value(productInfo.getProtocol()))
                .thenReturn(operator);
    }

    @Override
    public Mono<Void> unRegistry(String deviceId) {
        return manager.getStorage("device:" + deviceId)
                .flatMap(ConfigStorage::clear)
                .doOnSuccess(r -> operatorCache.invalidate(deviceId))
                .then();
    }

}
