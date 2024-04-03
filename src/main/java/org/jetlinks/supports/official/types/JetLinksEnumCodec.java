package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.types.DataTypes;
import org.jetlinks.core.metadata.types.EnumType;
import org.jetlinks.supports.official.JetLinksDataTypeCodecs;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksEnumCodec extends AbstractDataTypeCodec<EnumType> {

    @Override
    public String getTypeId() {
        return EnumType.ID;
    }

    @Override
    public EnumType decode(EnumType type, Map<String, Object> config) {
        super.decode(type, config);
        JSONObject jsonObject = new JSONObject(config);

        ofNullable(jsonObject.getJSONArray("elements"))
            .map(list -> list
                .stream()
                .map(JSONObject.class::cast)
                .map(e -> EnumType.Element.of(e.getString("value"), e.getString("text"), e.getString("description")))
                .collect(Collectors.toList()))
            .ifPresent(type::setElements);

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

                JetLinksDataTypeCodecs.getCodec(dataType.getId())
                                      .ifPresent(codec -> codec.decode(dataType, eleType));

                return dataType;
            })
            .ifPresent(type::setValueType);

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

        encoded.put("valueType", JetLinksDataTypeCodecs.encode(type.getValueType()).orElse(null));

    }
}
