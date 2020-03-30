package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.types.ArrayType;
import org.jetlinks.core.metadata.types.DataTypes;
import org.jetlinks.supports.official.JetLinksDataTypeCodecs;

import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksArrayCodec extends AbstractDataTypeCodec<ArrayType> {

    @Override
    public String getTypeId() {
        return ArrayType.ID;
    }

    @Override
    public ArrayType decode(ArrayType type, Map<String, Object> config) {
        super.decode(type, config);
        JSONObject jsonObject = new JSONObject(config);
        ofNullable(jsonObject.get("elementType"))
                .map(v -> {
                    if (v instanceof JSONObject) {
                        return ((JSONObject) v);
                    }
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
                .ifPresent(type::setElementType);

        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, ArrayType type) {
        super.doEncode(encoded, type);
        JetLinksDataTypeCodecs.getCodec(type.getId())
                .ifPresent(codec -> encoded.put("elementType", codec.encode(type.getElementType())));

    }
}
