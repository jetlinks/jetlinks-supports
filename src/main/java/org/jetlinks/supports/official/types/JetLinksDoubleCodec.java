package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.DoubleType;
import org.jetlinks.core.metadata.unit.ValueUnits;

import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksDoubleCodec extends AbstractDataTypeCodec<DoubleType> {

    @Override
    public String getTypeId() {
        return DoubleType.ID;
    }

    @Override
    public DoubleType decode(DoubleType type, Map<String, Object> config) {
        super.decode(type,config);
        JSONObject jsonObject = new JSONObject(config);
        ofNullable(jsonObject.getDouble("max"))
                .ifPresent(type::setMax);
        ofNullable(jsonObject.getDouble("min"))
                .ifPresent(type::setMin);
        ofNullable(jsonObject.getInteger("scale"))
                .ifPresent(type::setScale);
        ofNullable(jsonObject.getString("unit"))
                .flatMap(ValueUnits::lookup)
                .ifPresent(type::setUnit);
        return type;
    }

    @Override
    public Map<String, Object> encode(DoubleType type) {
        JSONObject json = new JSONObject();
        json.put("max", type.getMax());
        json.put("min", type.getMin());

        json.put("scale", type.getScale());
        if (type.getUnit() != null) {
            json.put("unit", type.getUnit().getId());
        }

        return json;
    }
}
