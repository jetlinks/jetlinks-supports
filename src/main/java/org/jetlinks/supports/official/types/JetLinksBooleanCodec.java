package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.BooleanType;

import java.util.Map;

import static java.util.Optional.ofNullable;


@Getter
@Setter
public class JetLinksBooleanCodec extends AbstractDataTypeCodec<BooleanType> {

    @Override
    public String getTypeId() {
        return BooleanType.ID;
    }

    @Override
    public BooleanType decode(BooleanType type, Map<String, Object> config) {
        super.decode(type,config);
        JSONObject jsonObject = new JSONObject(config);

        ofNullable(jsonObject.getString("trueText"))
                .ifPresent(type::setTrueText);
        ofNullable(jsonObject.getString("falseText"))
                .ifPresent(type::setFalseText);
        ofNullable(jsonObject.getString("trueValue"))
                .ifPresent(type::setTrueValue);
        ofNullable(jsonObject.getString("falseValue"))
                .ifPresent(type::setFalseValue);
        ofNullable(jsonObject.getString("description"))
                .ifPresent(type::setDescription);

        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, BooleanType type) {
        super.doEncode(encoded, type);
        encoded.put("trueText", type.getTrueText());
        encoded.put("falseText", type.getFalseText());
        encoded.put("trueValue", type.getTrueValue());
        encoded.put("falseValue", type.getFalseValue());

    }
}
