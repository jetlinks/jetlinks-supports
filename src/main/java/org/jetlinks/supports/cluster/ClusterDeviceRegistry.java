package org.jetlinks.supports.cluster;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.Setter;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.cache.Caches;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ClusterSet;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.defaults.DefaultDeviceOperator;
import org.jetlinks.core.defaults.DefaultDeviceProductOperator;
import org.jetlinks.core.device.*;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.core.things.ThingRpcSupportChain;
import org.jetlinks.supports.config.ClusterConfigStorageManager;
import org.springframework.util.ObjectUtils;
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

    @Setter
    private ThingRpcSupportChain rpcChain;

    @Deprecated
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

    @Deprecated
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
        if (ObjectUtils.isEmpty(deviceId)) {
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
            //有productId说明是存在的设备
            .getSelfConfig(DeviceConfigKey.productId)
            .doOnNext(r -> operatorCache.put(deviceId, deviceOperator
                .getSelfConfig(DeviceConfigKey.productId)
                .map(ignore -> deviceOperator)
                //设备被注销了？则移除之
                .switchIfEmpty(Mono.fromRunnable(() -> operatorCache.invalidate(deviceId)))
            ))
            .map(ignore -> deviceOperator);

    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId) {
        if (ObjectUtils.isEmpty(productId)) {
            return Mono.empty();
        }
        {
            DeviceProductOperator operator = productOperatorMap.get(productId);
            if (null != operator) {
                return Mono.just(operator);
            }
        }
        DefaultDeviceProductOperator deviceOperator = createProductOperator(productId);
        return deviceOperator
            .getConfig(DeviceConfigKey.protocol)
            .doOnNext(r -> productOperatorMap.put(productId, deviceOperator))
            .map((r) -> deviceOperator);
    }

    private String createProductCacheKey(String productId, String version) {
        return StringUtils.hasText(version) ? productId + ":" + version : productId;
    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId, String version) {

        if (ObjectUtils.isEmpty(productId)) {
            return Mono.empty();
        }
        if (ObjectUtils.isEmpty(version)) {
            return getProduct(productId);
        }
        String cacheId = createProductCacheKey(productId, version);

        {
            DeviceProductOperator operator = productOperatorMap.get(cacheId);
            if (null != operator) {
                return Mono.just(operator);
            }
        }
        DefaultDeviceProductOperator operator = createProductOperator(productId, version);
        return operator
            .getConfig(DeviceConfigKey.protocol)
            .doOnNext(r -> productOperatorMap.put(cacheId, operator))
            .map((r) -> operator);
    }

    private DefaultDeviceOperator createOperator(String deviceId) {
        DefaultDeviceOperator device = new DefaultDeviceOperator(deviceId, supports, manager, handler, this, interceptor, stateChecker);
        if (rpcChain != null) {
            device.setRpcChain(rpcChain);
        }
        return device;
    }

    private DefaultDeviceProductOperator createProductOperator(String id) {
        return new DefaultDeviceProductOperator(id,
                                                supports,
                                                manager,
                                                () -> getProductBind(id, null).values().flatMap(this::getDevice));
    }

    private DefaultDeviceProductOperator createProductOperator(String id, String version) {
        if (StringUtils.isEmpty(version)) {
            return createProductOperator(id);
        }
        String storageId = String.join(":", "device-product", id, version);
        return new DefaultDeviceProductOperator(id,
                                                supports,
                                                manager.getStorage(storageId),
                                                () -> getProductBind(id, version).values().flatMap(this::getDevice));
    }

    private ClusterSet<String> getProductBind(String id, String version) {
        return clusterManager
            .getSet(StringUtils.isEmpty(version) ? "device-product-bind:" + id : "device-product-bind:" + id + ":" + version);
    }

    @Override
    public Mono<DeviceOperator> register(DeviceInfo deviceInfo) {
        return Mono.defer(() -> {
            DefaultDeviceOperator operator = createOperator(deviceInfo.getId());
            operatorCache.put(operator.getDeviceId(), Mono
                .<DeviceOperator>just(operator)
                .filterWhen(device -> device.getSelfConfig(DeviceConfigKey.productId).hasElement()));

            Map<String, Object> configs = new HashMap<>();

            Optional.ofNullable(deviceInfo.getConfiguration())
                    .ifPresent(configs::putAll);

            Optional.ofNullable(deviceInfo.getMetadata())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.metadata.getKey(), conf));
            Optional.ofNullable(deviceInfo.getProtocol())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.protocol.getKey(), conf));
            Optional.ofNullable(deviceInfo.getProductId())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.productId.getKey(), conf));
            Optional.ofNullable(deviceInfo.getProductVersion())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.productVersion.getKey(), conf));

            return operator
                .setConfigs(configs)
                .then(operator.getProtocol())
                .flatMap(protocol -> protocol.onDeviceRegister(operator))
                //绑定设备到产品
                .then(getProductBind(deviceInfo.getProductId(), deviceInfo.getProductVersion()).add(deviceInfo.getId()))
                .thenReturn(operator);
        });
    }

    @Override
    public Mono<DeviceProductOperator> register(ProductInfo productInfo) {
        return Mono.defer(() -> {
            DefaultDeviceProductOperator operator = createProductOperator(productInfo.getId(), productInfo.getVersion());
            String cacheId = createProductCacheKey(productInfo.getId(), productInfo.getVersion());
            productOperatorMap.put(cacheId, operator);

            Map<String, Object> configs = new HashMap<>();

            Optional.ofNullable(productInfo.getConfiguration())
                    .ifPresent(configs::putAll);

            Optional.ofNullable(productInfo.getMetadata())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.metadata.getKey(), conf));

            Optional.ofNullable(productInfo.getProtocol())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.protocol.getKey(), conf));

            Optional.ofNullable(productInfo.getVersion())
                    .ifPresent(conf -> configs.put(DeviceConfigKey.productVersion.getKey(), conf));

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
            .flatMap(this::doUnregister)
            .doFinally(r -> operatorCache.invalidate(deviceId))
            .then();
    }

    @Override
    public Mono<Void> unregisterProduct(String productId) {
        return this
            .getProduct(productId)
            .flatMap(this::doUnregister)
            .doFinally(r -> productOperatorMap.remove(createProductCacheKey(productId, null)));

    }

    @Override
    public Mono<Void> unregisterProduct(String productId, String version) {
        return this
            .getProduct(productId, version)
            .flatMap(this::doUnregister)
            .doFinally(r -> productOperatorMap.remove(createProductCacheKey(productId, version)));
    }

    protected Mono<Void> doUnregister(DeviceProductOperator product) {
        return product
            .getProtocol()
            .flatMap(protocol -> protocol.onProductUnRegister(product))
            .then(
                product
                    .unwrap(DefaultDeviceProductOperator.class)
                    .getReactiveStorage()
                    .flatMap(ConfigStorage::clear)
            )
            .then();
    }

    protected Mono<Void> doUnregister(DeviceOperator device) {
        return device
            .getProtocol()
            .flatMap(protocol -> protocol.onDeviceUnRegister(device))
            .then(
                device
                    .unwrap(DefaultDeviceOperator.class)
                    .getReactiveStorage()
                    .flatMap(ConfigStorage::clear)
            )
            .then();
    }

    public void addInterceptor(DeviceMessageSenderInterceptor interceptor) {
        this.interceptor.addInterceptor(interceptor);
    }

    public void addStateChecker(DeviceStateChecker deviceStateChecker) {
        this.stateChecker.addDeviceStateChecker(deviceStateChecker);
    }

    public void addRpcChain(ThingRpcSupportChain chain) {
        if (this.rpcChain == null) {
            this.rpcChain = chain;
        } else {
            this.rpcChain = this.rpcChain.composite(Collections.singleton(chain));
        }
    }
}
