package org.jetlinks.supports.device;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * 设备模块实例快照信息.
 *
 * @author zhouhao
 * @since 1.3.2
 */
public class DeviceModuleInfo implements Serializable {

    private String id;

    private String deviceId;

    private String code;

    private String instanceCode;

    private Map<String, Object> configuration;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getInstanceCode() {
        return instanceCode;
    }

    public void setInstanceCode(String instanceCode) {
        this.instanceCode = instanceCode;
    }

    public Map<String, Object> getConfiguration() {
        return configuration == null ? Collections.emptyMap() : configuration;
    }

    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeviceModuleInfo)) {
            return false;
        }
        DeviceModuleInfo that = (DeviceModuleInfo) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
