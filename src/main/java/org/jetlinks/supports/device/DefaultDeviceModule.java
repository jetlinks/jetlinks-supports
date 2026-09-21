package org.jetlinks.supports.device;

import com.alibaba.fastjson.JSONObject;
import org.jetlinks.core.Value;
import org.jetlinks.core.Values;
import org.jetlinks.core.config.ConfigKeyValue;
import org.jetlinks.core.config.ConfigStorage;
import org.jetlinks.core.device.DeviceModule;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Headers;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.ThingMessage;
import org.jetlinks.core.message.module.DeviceModuleMessage;
import org.jetlinks.core.things.ThingMetadata;
import org.jetlinks.core.things.ThingTemplate;
import org.jetlinks.core.things.ThingType;
import org.jetlinks.supports.official.DefaultThingsMetadata;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 基于设备注册中心配置缓存的通用设备模块实现.
 *
 * @author zhouhao
 * @since 1.3.2
 */
public class DefaultDeviceModule implements DeviceModule {

    public static final ThingType TYPE = ThingType.of("device-module");

    public static final String METADATA_KEY = "metadata";

    private final DeviceOperator parent;

    private final DeviceModuleInfo module;

    private final Mono<ConfigStorage> storage;

    private final ThingTemplate template = new ModuleThingTemplate();

    public DefaultDeviceModule(DeviceOperator parent,
                               DeviceModuleInfo module,
                               Mono<ConfigStorage> storage) {
        this.parent = parent;
        this.module = module;
        this.storage = storage.cache();
    }

    private Mono<? extends org.jetlinks.core.metadata.DeviceMetadata> parentMetadata() {
        return Mono.defer(() -> {
            Mono<? extends org.jetlinks.core.metadata.DeviceMetadata> metadata = parent.getMetadata();
            return metadata == null ? Mono.empty() : metadata;
        });
    }

    @Override
    public String getDeviceId() {
        return module.getDeviceId() == null ? parent.getDeviceId() : module.getDeviceId();
    }

    @Override
    public String getCode() {
        return module.getCode();
    }

    @Override
    public String getInstanceCode() {
        return module.getInstanceCode();
    }

    @Override
    public String getId() {
        return module.getId();
    }

    @Override
    public ThingType getType() {
        return TYPE;
    }

    @Override
    public Mono<? extends ThingTemplate> getTemplate() {
        return Mono.just(template);
    }

    @Override
    public Mono<Void> resetMetadata() {
        return removeConfig(METADATA_KEY).then();
    }

    @Override
    public Mono<? extends ThingMetadata> getMetadata() {
        return getTemplateMetadata()
            .flatMap(this::mergeSelfMetadata);
    }

    private Mono<ThingMetadata> getTemplateMetadata() {
        return parentMetadata()
            .mapNotNull(metadata -> metadata.getModule(module.getCode()).orElse(null));
    }

    private Mono<ThingMetadata> mergeSelfMetadata(ThingMetadata base) {
        return getSelfConfig(METADATA_KEY)
            .mapNotNull(Value::get)
            .filter(this::hasMetadataValue)
            .map(this::convertMetadata)
            .map(base::merge)
            .defaultIfEmpty(base);
    }

    private boolean hasMetadataValue(Object metadata) {
        return !(metadata instanceof String) || StringUtils.hasText((String) metadata);
    }

    @SuppressWarnings("unchecked")
    private ThingMetadata convertMetadata(Object metadata) {
        if (metadata instanceof ThingMetadata) {
            return (ThingMetadata) metadata;
        }
        if (metadata instanceof JSONObject) {
            return new DefaultThingsMetadata((JSONObject) metadata);
        }
        if (metadata instanceof Map) {
            return new DefaultThingsMetadata(new JSONObject((Map<String, Object>) metadata));
        }
        if (metadata instanceof String && StringUtils.hasText((String) metadata)) {
            return new DefaultThingsMetadata(JSONObject.parseObject((String) metadata));
        }
        Object json = JSONObject.toJSON(metadata);
        return json instanceof JSONObject
            ? new DefaultThingsMetadata((JSONObject) json)
            : new DefaultThingsMetadata(new JSONObject());
    }

    @Override
    public Mono<Boolean> updateMetadata(String metadata) {
        return Mono.defer(() -> setConfig(METADATA_KEY, parseMetadata(metadata)));
    }

    @Override
    public Mono<Boolean> updateMetadata(ThingMetadata metadata) {
        return setConfig(METADATA_KEY, metadata.toJson());
    }

    private Object parseMetadata(String metadata) {
        return StringUtils.hasText(metadata) ? JSONObject.parseObject(metadata) : metadata;
    }

    @Override
    public Mono<Value> getSelfConfig(String key) {
        if (module.getConfiguration().containsKey(key)) {
            return Mono.just(Value.simple(module.getConfiguration().get(key)));
        }
        return storage.flatMap(store -> store.getConfig(key));
    }

    @Override
    public Mono<Values> getSelfConfigs(Collection<String> keys) {
        return storage
            .flatMap(store -> store.getConfigs(keys))
            .defaultIfEmpty(Values.of(Map.of()))
            .map(values -> {
                Map<String, Object> merged = new LinkedHashMap<>(module.getConfiguration());
                if (keys != null && !keys.isEmpty()) {
                    merged.keySet().retainAll(keys);
                }
                merged.putAll(values.getAllValues());
                return Values.of(merged);
            });
    }

    @Override
    public Mono<Value> getConfig(String key) {
        return getSelfConfig(key).switchIfEmpty(parent.getConfig(key));
    }

    @Override
    public Mono<Values> getConfigs(Collection<String> keys) {
        return parent
            .getConfigs(keys)
            .defaultIfEmpty(Values.of(Map.of()))
            .zipWith(getSelfConfigs(keys).defaultIfEmpty(Values.of(Map.of())), (parentValues, selfValues) -> {
                Map<String, Object> merged = new LinkedHashMap<>(parentValues.getAllValues());
                merged.putAll(selfValues.getAllValues());
                return Values.of(merged);
            });
    }

    @Override
    public Mono<Boolean> setConfig(String key, Object value) {
        return storage.flatMap(store -> store.setConfig(key, value));
    }

    @Override
    public Mono<Boolean> setConfig(ConfigKeyValue<?> keyValue) {
        return setConfig(keyValue.getKey(), keyValue.getValue());
    }

    @Override
    public Mono<Boolean> setConfigs(Map<String, Object> conf) {
        return storage.flatMap(store -> store.setConfigs(conf));
    }

    @Override
    public Mono<Boolean> removeConfig(String key) {
        return storage.flatMap(store -> store.remove(key));
    }

    @Override
    public Mono<Value> getAndRemoveConfig(String key) {
        return storage.flatMap(store -> store.getAndRemove(key));
    }

    @Override
    public Mono<Boolean> removeConfigs(Collection<String> key) {
        return storage.flatMap(store -> store.remove(key));
    }

    @Override
    public Mono<Void> refreshConfig(Collection<String> keys) {
        return storage.flatMap(store -> store.refresh(keys));
    }

    @Override
    public Mono<Void> refreshAllConfig() {
        return storage.flatMap(ConfigStorage::refresh);
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
        return type.isInstance(this) || parent.isWrapperFor(type);
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(this)) {
            return type.cast(this);
        }
        return parent.unwrap(type);
    }

    @Override
    public org.jetlinks.core.things.ThingRpcSupport rpc() {
        return message -> parent.rpc().call(wrap(message));
    }

    private DeviceModuleMessage wrap(ThingMessage message) {
        Message inner = message instanceof DeviceMessage
            ? message.copy()
            : message.copy().thingId(getType(), getId());
        DeviceModuleMessage wrapper = new DeviceModuleMessage();
        wrapper.deviceId(getDeviceId());
        wrapper.module(getCode());
        wrapper.moduleInstance(getInstanceCode());
        wrapper.message(inner);
        wrapper.timestamp(message.getTimestamp());
        wrapper.messageId(message.getMessageId());
        Map<String, Object> headers = message.getHeaders();
        if (headers != null) {
            headers.forEach(wrapper::addHeader);
        }
        wrapper.addHeader("module", getCode());
        wrapper.addHeader("moduleInstance", getInstanceCode());
        wrapper.addHeader(Headers.routeKey, getDeviceId());
        return wrapper;
    }

    private class ModuleThingTemplate implements ThingTemplate {

        @Override
        public String getId() {
            return getCode();
        }

        @Override
        public Mono<? extends ThingMetadata> getMetadata() {
            return getTemplateMetadata();
        }

        @Override
        public Mono<Boolean> updateMetadata(String metadata) {
            return Mono.error(new UnsupportedOperationException("unsupported update device module template metadata"));
        }

        @Override
        public Mono<Boolean> updateMetadata(ThingMetadata metadata) {
            return Mono.error(new UnsupportedOperationException("unsupported update device module template metadata"));
        }

        @Override
        public Mono<Value> getConfig(String key) {
            return parent.getConfig(key);
        }

        @Override
        public Mono<Values> getConfigs(Collection<String> keys) {
            return parent.getConfigs(keys);
        }

        @Override
        public Mono<Boolean> setConfig(String key, Object value) {
            return Mono.error(new UnsupportedOperationException("unsupported update device module template config"));
        }

        @Override
        public Mono<Boolean> setConfigs(Map<String, Object> conf) {
            return Mono.error(new UnsupportedOperationException("unsupported update device module template config"));
        }

        @Override
        public Mono<Boolean> removeConfig(String key) {
            return Mono.error(new UnsupportedOperationException("unsupported update device module template config"));
        }

        @Override
        public Mono<Value> getAndRemoveConfig(String key) {
            return Mono.error(new UnsupportedOperationException("unsupported update device module template config"));
        }

        @Override
        public Mono<Boolean> removeConfigs(Collection<String> key) {
            return Mono.error(new UnsupportedOperationException("unsupported update device module template config"));
        }

        @Override
        public Mono<Void> refreshConfig(Collection<String> keys) {
            return parent.refreshConfig(keys);
        }

        @Override
        public Mono<Void> refreshAllConfig() {
            return parent.refreshAllConfig();
        }
    }
}
