package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.cache.Caches;
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
import java.util.*;

public class ClusterDeviceRegistry implements DeviceRegistry {
    //全局拦截器
    private final CompositeDeviceMessageSenderInterceptor interceptor = new CompositeDeviceMessageSenderInterceptor();

    //配置管理器
    private final ConfigStorageManager manager;

    //缓存
    private final Cache<String, Mono<DeviceOperator>> operatorCache;

    //产品
    private final Map<String, DeviceProductOperator> productOperatorMap = Caches.newCache();

    //协议支持
    private final ProtocolSupports supports;

    //设备操作
    private final DeviceOperationBroker handler;

    //集群管理
    private final ClusterManager clusterManager;

    //状态检查器
    private final CompositeDeviceStateChecker stateChecker = new CompositeDeviceStateChecker();

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
                                 ConfigStorageManager storageManager,
                                 ClusterManager clusterManager,
                                 DeviceOperationBroker handler,
                                 Cache<String, Mono<DeviceOperator>> cache) {
        this.supports = supports;
        this.handler = handler;
        this.manager = storageManager;
        this.operatorCache = cache;
        this.clusterManager = clusterManager;
        this.addStateChecker(DefaultDeviceOperator.DEFAULT_STATE_CHECKER);
    }

    public ClusterDeviceRegistry(ProtocolSupports supports,
                                 ClusterManager clusterManager,
                                 DeviceOperationBroker handler,
                                 Cache<String, Mono<DeviceOperator>> cache) {
        this.supports = supports;
        this.handler = handler;
        this.manager = new ClusterConfigStorageManager(clusterManager);
        this.operatorCache = cache;
        this.clusterManager = clusterManager;
        this.addStateChecker(DefaultDeviceOperator.DEFAULT_STATE_CHECKER);
    }

    @Override
    public Flux<DeviceStateInfo> checkDeviceState(Flux<? extends Collection<String>> id) {

        return id.flatMap(list -> Flux
                .fromIterable(list)
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
        {

            Mono<DeviceOperator> deviceOperator = operatorCache.getIfPresent(deviceId);
            if (null != deviceOperator) {
                return deviceOperator;
            }
        }
        DeviceOperator deviceOperator = createOperator(deviceId);
        return deviceOperator
                .getSelfConfig(DeviceConfigKey.productId)
                .doOnNext(r -> operatorCache.put(deviceId, Mono
                        .just(deviceOperator)
                        .filterWhen(device -> device.getSelfConfig(DeviceConfigKey.productId).hasElement())
                ))
                .map(ignore -> deviceOperator);

    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId) {
        if (StringUtils.isEmpty(productId)) {
            return Mono.empty();
        }
        {
            DeviceProductOperator operator = productOperatorMap.get(productId);
            if (null != operator) {
                return Mono.just(operator);
            }
        }
        DeviceProductOperator deviceOperator = createProductOperator(productId);
        return deviceOperator
                .getConfig(DeviceConfigKey.protocol)
                .doOnNext(r -> productOperatorMap.put(productId, deviceOperator))
                .map((r) -> deviceOperator);
    }


    private DefaultDeviceOperator createOperator(String deviceId) {
        return new DefaultDeviceOperator(deviceId, supports, manager, handler, this, interceptor, stateChecker);
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
            operatorCache.put(operator.getDeviceId(), Mono
                    .<DeviceOperator>just(operator)
                    .filterWhen(device -> device.getSelfConfig(DeviceConfigKey.productId).hasElement()));

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
                           .then(operator.getProtocol())
                           .flatMap(protocol -> protocol.onDeviceRegister(operator))
                           //绑定设备到产品
                           .then(clusterManager.<String>getSet("device-product-bind:" + deviceInfo.getProductId()).add(deviceInfo
                                                                                                                               .getId()))
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

            return operator
                    .setConfigs(configs)
                    .then(operator.getProtocol())
                    .flatMap(protocol -> protocol.onProductRegister(operator))
                    .thenReturn(operator);
        });
    }

    @Override
    public Mono<Void> unregisterDevice(String deviceId) {
        return this
                .getDevice(deviceId)
                .flatMap(device -> device
                        .getProtocol()
                        .flatMap(protocol -> protocol.onDeviceUnRegister(device)))
                .then(
                        manager.getStorage("device:" + deviceId)
                               .flatMap(ConfigStorage::clear)
                )
                .doFinally(r -> operatorCache.invalidate(deviceId))
                .then();
    }

    @Override
    public Mono<Void> unregisterProduct(String productId) {
        return this
                .getProduct(productId)
                .flatMap(product -> product
                        .getProtocol()
                        .flatMap(protocol -> protocol.onProductUnRegister(product)))
                .then(
                        manager.getStorage("device-product:" + productId)
                               .flatMap(ConfigStorage::clear)
                )
                .doFinally(s -> productOperatorMap.remove(productId))
                .then();

    }

    public void addInterceptor(DeviceMessageSenderInterceptor interceptor) {
        this.interceptor.addInterceptor(interceptor);
    }

    public void addStateChecker(DeviceStateChecker deviceStateChecker) {
        this.stateChecker.addDeviceStateChecker(deviceStateChecker);
    }
}
