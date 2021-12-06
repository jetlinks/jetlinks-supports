package org.jetlinks.supports.test;

import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.defaults.DefaultDeviceOperator;
import org.jetlinks.core.defaults.DefaultDeviceProductOperator;
import org.jetlinks.core.device.*;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.supports.config.InMemoryConfigStorageManager;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDeviceRegistry implements DeviceRegistry {

    private final CompositeDeviceMessageSenderInterceptor interceptor = new CompositeDeviceMessageSenderInterceptor();

    private final ConfigStorageManager manager = new InMemoryConfigStorageManager();

    private final Map<String, DeviceOperator> operatorMap = new ConcurrentHashMap<>();

    private final Map<String, DeviceProductOperator> productOperatorMap = new ConcurrentHashMap<>();

    private final ProtocolSupports supports;

    private final DeviceOperationBroker handler;

    public static InMemoryDeviceRegistry create() {
        return new InMemoryDeviceRegistry();
    }

    public InMemoryDeviceRegistry() {
        this(new MockProtocolSupport(), new StandaloneDeviceMessageBroker());
    }

    public InMemoryDeviceRegistry(ProtocolSupports supports, DeviceOperationBroker handler) {
        this.supports = supports;
        this.handler = handler;
    }


    @Override
    public Mono<DeviceOperator> getDevice(String deviceId) {
        return Mono.fromSupplier(() -> operatorMap.get(deviceId));
    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId) {
        return Mono.fromSupplier(() -> productOperatorMap.get(productId));
    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId, String version) {
        return Mono.fromSupplier(() -> productOperatorMap.get(productId + ":" + version));
    }

    @Override
    public Mono<DeviceOperator> register(DeviceInfo deviceInfo) {
        return Mono.defer(() -> {
            DefaultDeviceOperator operator = new DefaultDeviceOperator(
                    deviceInfo.getId(),
                    supports, manager, handler, this, interceptor
            );
            operatorMap.put(operator.getDeviceId(), operator);

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
            String cacheId = StringUtils.isEmpty(productInfo.getVersion())
                    ? productInfo.getId()
                    : productInfo.getId() + ":" + productInfo.getVersion();

            DefaultDeviceProductOperator operator = new DefaultDeviceProductOperator(productInfo.getId(),
                                                                                     supports,
                                                                                     manager.getStorage(cacheId),
                                                                                     Flux::empty);
            productOperatorMap.put(cacheId, operator);
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
        return Mono.justOrEmpty(deviceId)
                   .map(operatorMap::remove)
                   .then();
    }

    @Override
    public Mono<Void> unregisterProduct(String productId) {
        return Mono.justOrEmpty(productId)
                   .map(productOperatorMap::remove)
                   .then();
    }

    @Override
    public Mono<Void> unregisterProduct(String productId, String version) {
        return Mono.justOrEmpty(productId + ":" + version)
                   .map(productOperatorMap::remove)
                   .then();
    }

    public void addInterceptor(DeviceMessageSenderInterceptor interceptor) {
        this.interceptor.addInterceptor(interceptor);
    }
}
