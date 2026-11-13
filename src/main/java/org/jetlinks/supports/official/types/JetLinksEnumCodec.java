package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.EnumType;
import org.jetlinks.supports.official.JetLinksDataTypeCodecs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

/**
 * 默认枚举类型 Codec，读取枚举选项与可选值类型，不加载业务对象。
 * 值类型与数组元素共用类型解析边界，保留类型名简写并拒绝显式无效定义。
 *
 * @see JetLinksDataTypeCodecs#decodeNow(Object)
 */
@Getter
@Setter
public class JetLinksEnumCodec extends AbstractDataTypeCodec<EnumType> {

    @Override
    public String getTypeId() {
        return EnumType.ID;
    }

    /**
     * 保留枚举选项原始空值，避免传输往返产生虚假的配置差异。
     */
    @Override
    public EnumType decode(EnumType type, Map<String, Object> config) {
        super.decode(type, config);
        JSONObject jsonObject = new JSONObject(config);

        JSONArray arr = jsonObject.getJSONArray("elements");
        if (arr != null) {
            List<EnumType.Element> elements = new ArrayList<>(arr.size());
            for (Object obj : arr) {
                if (obj instanceof Map) {
                    elements.add(
                        EnumType.Element.of(
                            Maps.transformValues((Map<String, Object>) obj, value -> Objects.toString(value, null))
                        )
                    );
                } else if (obj instanceof EnumType.Element) {
                    elements.add((EnumType.Element) obj);
                }
            }
            type.setElements(elements);
        }


        ofNullable(config.get("valueType"))
            .map(JetLinksDataTypeCodecs::decodeNow)
            .ifPresent(type::setValueType);

        type.setMulti(jsonObject.getBooleanValue("multi"));

        return type;
    }

    /**
     * 值类型和多选配置不依赖选项列表，未声明选项时仍需完整编码。
     */
    @Override
    protected void doEncode(Map<String, Object> encoded, EnumType type) {
        super.doEncode(encoded, type);
        if (type.getElements() != null) {
            encoded.put("elements", type
                .getElements()
                .stream()
                .map(EnumType.Element::toMap)
                .collect(Collectors.toList()));
        }

        encoded.put("valueType", JetLinksDataTypeCodecs
            .encode(type.getValueType())
            .orElse(null));

        encoded.put("multi", type.isMulti());

    }
}
