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
    //全局拦截器
    private final CompositeDeviceMessageSenderInterceptor interceptor = new CompositeDeviceMessageSenderInterceptor();

    //配置管理器
    private final ConfigStorageManager manager;

    //缓存
    private final Cache<String, DeviceOperator> operatorCache;

    //产品
    private final Map<String, DeviceProductOperator> productOperatorMap = new ConcurrentHashMap<>();

    //协议支持
    private final ProtocolSupports supports;

    //设备操作
    private final DeviceOperationBroker handler;

    //集群管理
    private final ClusterManager clusterManager;

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
        this.clusterManager = clusterManager;
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
        if (StringUtils.isEmpty(deviceId)) {
            return Mono.empty();
        }
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
        if (StringUtils.isEmpty(productId)) {
            return Mono.empty();
        }
        return Mono.justOrEmpty(productOperatorMap.get(productId))
                .switchIfEmpty(Mono.defer(() -> {
                    DeviceProductOperator deviceOperator = createProductOperator(productId);
                    return deviceOperator.getConfig(DeviceConfigKey.protocol)
                            .doOnNext(r -> productOperatorMap.put(productId, deviceOperator))
                            .map((r) -> deviceOperator);
                }));
    }


    private DefaultDeviceOperator createOperator(String deviceId) {
        return new DefaultDeviceOperator(deviceId, supports, manager, handler, this, interceptor);
    }

    private DefaultDeviceProductOperator createProductOperator(String id) {
        return new DefaultDeviceProductOperator(id, supports, manager, () -> clusterManager
                .<String>getSet("device-product-bind:" + id)
                .values()
                .flatMap(this::getDevice));
    }

    @Override
    public Mono<DeviceOperator> register(DeviceInfo deviceInfo) {
        return Mono.defer(() -> {
            DefaultDeviceOperator operator = createOperator(deviceInfo.getId());
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

            return operator.setConfigs(configs)
                    //绑定设备到产品
                    .then(clusterManager.<String>getSet("device-product-bind:" + deviceInfo.getProductId()).add(deviceInfo.getId()))
                    .thenReturn(operator);
        });
    }

    @Override
    public Mono<DeviceProductOperator> register(ProductInfo productInfo) {
        return Mono.defer(() -> {
            DefaultDeviceProductOperator operator = createProductOperator(productInfo.getId());
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

    public void addInterceptor(DeviceMessageSenderInterceptor interceptor) {
        this.interceptor.addInterceptor(interceptor);
    }
}
