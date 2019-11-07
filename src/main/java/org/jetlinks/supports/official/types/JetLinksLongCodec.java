package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.LongType;
import org.jetlinks.core.metadata.unit.ValueUnits;

import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksLongCodec extends AbstractDataTypeCodec<LongType> {

    @Override
    public String getTypeId() {
        return LongType.ID;
    }

    @Override
    public LongType decode(LongType type, Map<String, Object> config) {
        super.decode(type,config);
        JSONObject jsonObject = new JSONObject(config);
        ofNullable(jsonObject.getInteger("max"))
                .ifPresent(type::setMax);
        ofNullable(jsonObject.getInteger("min"))
                .ifPresent(type::setMin);
        ofNullable(jsonObject.getString("unit"))
                .flatMap(ValueUnits::lookup)
                .ifPresent(type::setUnit);
        return type;
    }


    @Override
    protected void doEncode(Map<String, Object> encoded, LongType type) {
        encoded.put("max", type.getMax());
        encoded.put("min", type.getMin());
        if (type.getUnit() != null) {
            encoded.put("unit", type.getUnit().getId());
        }
    }
}
