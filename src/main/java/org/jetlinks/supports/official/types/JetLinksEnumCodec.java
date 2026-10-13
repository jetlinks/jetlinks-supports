package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.types.DataTypes;
import org.jetlinks.core.metadata.types.EnumType;
import org.jetlinks.supports.official.JetLinksDataTypeCodecs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

@Getter
@Setter
@Deprecated
public class JetLinksEnumCodec extends AbstractDataTypeCodec<EnumType> {

    @Override
    public String getTypeId() {
        return EnumType.ID;
    }

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
                            Maps.transformValues((Map<String, Object>) obj, String::valueOf)
                        )
                    );
                } else if (obj instanceof EnumType.Element) {
                    elements.add((EnumType.Element) obj);
                }
            }
            type.setElements(elements);
        }


        ofNullable(jsonObject.get("valueType"))
            .map(v -> {
                if (v instanceof Map) {
                    return new JSONObject(((Map<String, Object>) v));
                }
                // {valueType:int}
                JSONObject eleType = new JSONObject();
                eleType.put("type", v);
                return eleType;
            })
            .map(eleType -> {
                DataType dataType = DataTypes.lookup(eleType.getString("type")).get();

                JetLinksDataTypeCodecs
                    .getCodec(dataType.getId())
                    .ifPresent(codec -> codec.decode(dataType, eleType));

                return dataType;
            })
            .ifPresent(type::setValueType);

        type.setMulti(jsonObject.getBooleanValue("multi"));

        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, EnumType type) {
        super.doEncode(encoded, type);
        if (type.getElements() == null) {
            return;
        }
        encoded.put("elements", type
            .getElements()
            .stream()
            .map(EnumType.Element::toMap).collect(Collectors.toList()));

        encoded.put("valueType", Optional.ofNullable(type.getValueType()).map(DataType::toJson).orElse(null));

        encoded.put("multi", type.isMulti());

    }
}
