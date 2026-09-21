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
import org.jetlinks.core.device.DevicePrincipalManager;
import org.jetlinks.core.principal.Principal;
import org.jetlinks.core.message.interceptor.DeviceMessageSenderInterceptor;
import org.jetlinks.core.things.ThingRpcSupportChain;
import org.jetlinks.supports.config.ClusterConfigStorageManager;
import org.jetlinks.supports.config.CacheNotify;
import org.jetlinks.supports.config.ConfigStorageCacheNotifier;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterDeviceRegistry implements DeviceRegistry, Disposable {
    //全局拦截器
    private final CompositeDeviceMessageSenderInterceptor interceptor = new CompositeDeviceMessageSenderInterceptor();

    //配置管理器
    private final ConfigStorageManager manager;

    //缓存
    private final Cache<String, Mono<DeviceOperator>> operatorCache;

    private final boolean validatedDeviceCacheEnabled;

    //产品
    private final ConcurrentMap<String, DeviceProductOperator> productOperatorMap = Caches.newCache();

    //带版本产品
    private final ConcurrentMap<String, VersionedProductOperators> versionedProductOperatorMap = Caches.newCache();

    private final AtomicLong productCacheVersion = new AtomicLong();

    //协议支持
    private final ProtocolSupports supports;

    //设备操作
    private final DeviceOperationBroker handler;

    //集群管理
    private final ClusterManager clusterManager;

    private final Disposable cacheNotifyDisposable;

    private final List<DeviceMessageSenderInterceptor> registeredInterceptors = new CopyOnWriteArrayList<>();

    //状态检查器
    private final CompositeDeviceStateChecker stateChecker = new CompositeDeviceStateChecker();

    @Setter
    private ThingRpcSupportChain rpcChain;

    @Setter
    private DevicePrincipalManager principalManager;

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
        this.validatedDeviceCacheEnabled = storageManager instanceof ConfigStorageCacheNotifier;
        this.cacheNotifyDisposable = storageManager instanceof ConfigStorageCacheNotifier
            ? ((ConfigStorageCacheNotifier) storageManager).listenCacheNotify(this::handleConfigCacheNotify)
            : () -> {};
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
        this.validatedDeviceCacheEnabled = false;
        this.cacheNotifyDisposable = () -> {};
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
        if (validatedDeviceCacheEnabled) {
            return new MonoValidatedDeviceOperator(
                deviceId,
                deviceOperator,
                deviceOperator.getProduct(),
                operatorCache
            );
        }
        return deviceOperator
            //有产品则认为存在
            .getProduct()
            .doOnNext(r -> operatorCache.put(deviceId, deviceOperator
                .getProduct()
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
        long cacheVersion = productCacheVersion.get();
        return deviceOperator
            .getConfig(DeviceConfigKey.protocol)
            .doOnNext(r -> cacheProduct(productId, null, deviceOperator, cacheVersion))
            .map((r) -> deviceOperator);
    }

    @Override
    public Mono<DeviceProductOperator> getProduct(String productId, String version) {

        if (ObjectUtils.isEmpty(productId)) {
            return Mono.empty();
        }
        if (ObjectUtils.isEmpty(version)) {
            return getProduct(productId);
        }
        {
            DeviceProductOperator operator = getProductFromCache(productId, version);
            if (null != operator) {
                return Mono.just(operator);
            }
        }
        DefaultDeviceProductOperator operator = createProductOperator(productId, version);
        long cacheVersion = productCacheVersion.get();
        return operator
            .getConfig(DeviceConfigKey.protocol)
            .doOnNext(r -> cacheProduct(productId, version, operator, cacheVersion))
            .map((r) -> operator);
    }

    private DeviceProductOperator getProductFromCache(String productId, String version) {
        if (!StringUtils.hasText(version)) {
            return productOperatorMap.get(productId);
        }
        VersionedProductOperators versioned = versionedProductOperatorMap.get(productId);
        return versioned == null ? null : versioned.get(version);
    }

    private void cacheProduct(String productId, String version, DeviceProductOperator operator) {
        cacheProduct(productId, version, operator, productCacheVersion.get());
    }

    private void cacheProduct(String productId, String version, DeviceProductOperator operator, long cacheVersion) {
        if (productCacheVersion.get() != cacheVersion) {
            return;
        }
        if (!StringUtils.hasText(version)) {
            productOperatorMap.put(productId, operator);
            return;
        }
        versionedProductOperatorMap.compute(productId, (ignore, versioned) -> {
            if (versioned == null) {
                versioned = VersionedProductOperators.EMPTY;
            }
            return versioned.with(version, operator);
        });
    }

    private void removeProductFromCache(String productId, String version) {
        if (!StringUtils.hasText(version)) {
            productOperatorMap.remove(productId);
            return;
        }
        versionedProductOperatorMap.computeIfPresent(productId, (ignore, versioned) -> {
            VersionedProductOperators updated = versioned.without(version);
            return updated.isEmpty() ? null : updated;
        });
    }

    private DefaultDeviceOperator createOperator(String deviceId) {
        DefaultDeviceOperator device = new DefaultDeviceOperator(deviceId, supports, manager, handler, this, interceptor, stateChecker);
        if (rpcChain != null) {
            device.setRpcChain(rpcChain);
        }
        if (principalManager != null) {
            device.setPrincipalManager(principalManager);
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
                .thenReturn(operator)
                .doOnNext(this::cacheRegisteredDevice);
        });
    }

    @Override
    public Mono<DeviceProductOperator> register(ProductInfo productInfo) {
        return Mono.defer(() -> {
            DefaultDeviceProductOperator operator = createProductOperator(productInfo.getId(), productInfo.getVersion());

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
                .thenReturn(operator)
                .doOnNext(ignore -> cacheProduct(productInfo.getId(), productInfo.getVersion(), operator));
        });
    }

    @Override
    public Mono<Void> unregisterDevice(String deviceId) {
        return this
            .getDevice(deviceId)
            .flatMap(this::doUnregister)
            .doFinally(r -> invalidateDeviceCache(deviceId))
            .then();
    }

    @Override
    public Mono<Void> unregisterProduct(String productId) {
        return this
            .getProduct(productId)
            .flatMap(this::doUnregister)
            .doFinally(r -> {
                removeProductFromCache(productId, null);
                invalidateAllDeviceCache();
            });

    }

    @Override
    public Mono<Void> unregisterProduct(String productId, String version) {
        return this
            .getProduct(productId, version)
            .flatMap(this::doUnregister)
            .doFinally(r -> {
                removeProductFromCache(productId, version);
                invalidateAllDeviceCache();
            });
    }

    private void cacheRegisteredDevice(DeviceOperator operator) {
        Mono<DeviceOperator> cached;
        if (validatedDeviceCacheEnabled) {
            cached = new MonoValidatedDeviceOperator(
                operator.getDeviceId(),
                operator,
                operator.getSelfConfig(DeviceConfigKey.productId),
                operatorCache
            );
        } else {
            cached = Mono
                .just(operator)
                .filterWhen(device -> device.getSelfConfig(DeviceConfigKey.productId).hasElement());
        }
        operatorCache.put(operator.getDeviceId(), cached);
    }

    private void handleConfigCacheNotify(CacheNotify notify) {
        String storageId = notify.getName();
        if (!StringUtils.hasText(storageId)) {
            return;
        }
        if (storageId.startsWith("device:")) {
            if (affects(notify,
                        DeviceConfigKey.productId.getKey(),
                        DeviceConfigKey.productVersion.getKey())) {
                invalidateDeviceCache(storageId.substring("device:".length()));
            }
            return;
        }
        if (storageId.startsWith("device-product:")
            && affects(notify, DeviceConfigKey.protocol.getKey())) {
            invalidateProductCache();
            invalidateAllDeviceCache();
        }
    }

    private void invalidateProductCache() {
        productCacheVersion.incrementAndGet();
        productOperatorMap.clear();
        versionedProductOperatorMap.clear();
    }

    private boolean affects(CacheNotify notify, String... keys) {
        Collection<String> changed = notify.getKeys();
        if (notify.isClear() || changed == null || changed.isEmpty()) {
            return true;
        }
        for (String key : keys) {
            if (changed.contains(key)) {
                return true;
            }
        }
        return false;
    }

    private void invalidateDeviceCache(String deviceId) {
        Mono<DeviceOperator> cached = operatorCache.getIfPresent(deviceId);
        if (cached instanceof MonoValidatedDeviceOperator) {
            ((MonoValidatedDeviceOperator) cached).invalidate();
        }
        if (cached != null) {
            operatorCache.asMap().remove(deviceId, cached);
        } else {
            operatorCache.invalidate(deviceId);
        }
    }

    private void invalidateAllDeviceCache() {
        for (Mono<DeviceOperator> cached : operatorCache.asMap().values()) {
            if (cached instanceof MonoValidatedDeviceOperator) {
                ((MonoValidatedDeviceOperator) cached).invalidate();
            }
        }
        operatorCache.invalidateAll();
    }

    @Override
    public void dispose() {
        cacheNotifyDisposable.dispose();
        for (DeviceMessageSenderInterceptor interceptor : registeredInterceptors) {
            if (interceptor instanceof Disposable) {
                ((Disposable) interceptor).dispose();
            }
        }
        registeredInterceptors.clear();
        invalidateProductCache();
        invalidateAllDeviceCache();
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

    @Override
    public Mono<DevicePrincipal> resolveDevice(Principal query) {
        if (principalManager != null) {
            return principalManager.resolveDevicePrincipal(query);
        }
        return DeviceRegistry.super.resolveDevice(query);
    }

    public void addInterceptor(DeviceMessageSenderInterceptor interceptor) {
        registeredInterceptors.add(interceptor);
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

    private static final class VersionedProductOperators {

        private static final VersionedProductOperators EMPTY = new VersionedProductOperators(new Object[0]);

        private final Object[] operators;

        private VersionedProductOperators(Object[] operators) {
            this.operators = operators;
        }

        private DeviceProductOperator get(String version) {
            Object[] current = operators;
            for (int index = 0; index < current.length; index += 2) {
                if (version.equals(current[index])) {
                    return (DeviceProductOperator) current[index + 1];
                }
            }
            return null;
        }

        private VersionedProductOperators with(String version, DeviceProductOperator operator) {
            for (int index = 0; index < operators.length; index += 2) {
                if (version.equals(operators[index])) {
                    Object[] updated = operators.clone();
                    updated[index + 1] = operator;
                    return new VersionedProductOperators(updated);
                }
            }
            Object[] updated = Arrays.copyOf(operators, operators.length + 2);
            updated[operators.length] = version;
            updated[operators.length + 1] = operator;
            return new VersionedProductOperators(updated);
        }

        private VersionedProductOperators without(String version) {
            for (int index = 0; index < operators.length; index += 2) {
                if (!version.equals(operators[index])) {
                    continue;
                }
                if (operators.length == 2) {
                    return EMPTY;
                }
                Object[] updated = new Object[operators.length - 2];
                System.arraycopy(operators, 0, updated, 0, index);
                System.arraycopy(operators, index + 2, updated, index, operators.length - index - 2);
                return new VersionedProductOperators(updated);
            }
            return this;
        }

        private boolean isEmpty() {
            return operators.length == 0;
        }
    }
}
