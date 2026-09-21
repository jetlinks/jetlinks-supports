package org.jetlinks.supports.device;

import org.jetlinks.core.config.ConfigKey;
import org.jetlinks.core.config.ConfigStorageManager;
import org.jetlinks.core.device.DeviceModule;
import org.jetlinks.core.device.DeviceModuleThingProvider;
import org.jetlinks.core.device.DeviceOperator;
import org.springframework.core.ResolvableType;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 通用设备模块物提供器.
 * <p>
 * 在微服务运行时没有设备管理本地服务时,可直接基于父设备物模型缓存和模块实例配置缓存
 * 构造 {@link DeviceModule}。模块定义来自父设备物模型 {@code modules[id]}, 模块实例列表
 * 来自父设备配置 {@link #MODULES_CONFIG_KEY}, 模块实例配置存储使用平台内部稳定模块实例ID。
 *
 * @author zhouhao
 * @since 1.3.2
 */
public class DefaultDeviceModuleThingProvider implements DeviceModuleThingProvider {

    /**
     * 设备模块实例快照配置。
     */
    public static final ConfigKey<List<DeviceModuleInfo>> MODULES_CONFIG_KEY = ConfigKey.of(
        "device.modules",
        "设备模块实例列表",
        ResolvableType.forClassWithGenerics(List.class, DeviceModuleInfo.class).getType()
    );

    private final ConfigStorageManager storageManager;

    public DefaultDeviceModuleThingProvider(ConfigStorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public Flux<DeviceModule> getModuleThings(DeviceOperator device, String code) {
        return device
            .getMetadata()
            .filter(metadata -> metadata.getModule(code).isPresent())
            .flatMapMany(ignore -> getModuleInstances(device, code)
                .map(module -> newModule(device, module)));
    }

    @Override
    public Mono<DeviceModule> getModuleThing(DeviceOperator device, String code, String instanceCode) {
        String actualInstanceCode = Objects.requireNonNullElse(instanceCode, code);
        return device
            .getMetadata()
            .filter(metadata -> metadata.getModule(code).isPresent())
            .flatMap(ignore -> getModuleInstances(device, code)
                .filter(module -> actualInstanceCode.equals(module.getInstanceCode()))
                .next()
                .switchIfEmpty(Mono.fromSupplier(() -> fallback(device, code, actualInstanceCode)))
                .map(module -> newModule(device, module)));
    }

    protected Flux<DeviceModuleInfo> getModuleInstances(DeviceOperator device, String code) {
        return device
            .getConfig(MODULES_CONFIG_KEY)
            .flatMapMany(Flux::fromIterable)
            .filter(module -> code.equals(module.getCode()))
            .filter(module -> StringUtils.hasText(module.getInstanceCode()))
            .switchIfEmpty(Flux.defer(() -> Flux.just(fallback(device, code, code))));
    }

    protected DeviceModuleInfo fallback(DeviceOperator device, String code, String instanceCode) {
        DeviceModuleInfo info = new DeviceModuleInfo();
        info.setId(device.getDeviceId() + ":" + instanceCode);
        info.setDeviceId(device.getDeviceId());
        info.setCode(code);
        info.setInstanceCode(instanceCode);
        return info;
    }

    protected DeviceModule newModule(DeviceOperator device, DeviceModuleInfo module) {
        return new DefaultDeviceModule(
            device,
            module,
            storageManager.getStorage(createStorageId(module))
        );
    }

    protected String createStorageId(DeviceModuleInfo module) {
        return "device-module:" + module.getId();
    }
}
