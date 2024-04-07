package org.jetlinks.supports.utils;

import org.jetlinks.core.metadata.*;
import org.jetlinks.supports.official.DeviceMetadataParser;
import org.springframework.core.ResolvableType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 设备物模型工具类
 *
 * @author zhouhao
 * @since 1.2
 */
public class DeviceMetadataUtils {


    /**
     * 根据一个Map作为数据模版转为物模型属性信息
     *
     * @param value map 数据模版
     * @return 属性信息
     */
    public static List<PropertyMetadata> convertToProperties(Map<String, Object> value) {
        return value
            .entrySet()
            .stream()
            .filter(e -> e.getValue() != null)
            .map(entry -> {
                SimplePropertyMetadata metadata = new SimplePropertyMetadata();
                metadata.setId(entry.getKey());
                metadata.setName(entry.getKey());
                metadata.setValueType(DeviceMetadataParser.withType(
                    ResolvableType.forType(entry.getValue().getClass())
                ));
                return metadata;
            })
            .collect(Collectors.toList());
    }


    /**
     * 获取left中定义了但是right中没有定义的物模型.
     * <pre>{@code
     *
     *    new CompositeDeviceMetadata(left,right).difference();
     *
     * }</pre>
     *
     * @return DeviceMetadata
     * @param left 基准物模型
     * @param right 对比物模型
     * @since 1.2.2
     */
    public static DeviceMetadata difference(DeviceMetadata left, DeviceMetadata right) {
        SimpleDeviceMetadata metadata = new SimpleDeviceMetadata();
        metadata.setId(right.getId());
        metadata.setName(right.getName());
        metadata.setExpands(right.getExpands());
        metadata.setDescription(right.getDescription());

        for (PropertyMetadata meta : right.getProperties()) {
            if (left.getPropertyOrNull(meta.getId()) == null) {
                metadata.addProperty(meta);
            }
        }

        for (EventMetadata meta : right.getEvents()) {
            if (left.getEventOrNull(meta.getId()) == null) {
                metadata.addEvent(meta);
            }
        }

        for (FunctionMetadata meta : right.getFunctions()) {
            if (left.getFunctionOrNull(meta.getId()) == null) {
                metadata.addFunction(meta);
            }
        }

        for (PropertyMetadata meta : right.getTags()) {
            if (left.getTagOrNull(meta.getId()) == null) {
                metadata.addTag(meta);
            }
        }

        return metadata;
    }

}
