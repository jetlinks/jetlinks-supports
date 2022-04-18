package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.UserType;

import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksUserCodec extends AbstractDataTypeCodec<UserType> {

    @Override
    public String getTypeId() {
        return UserType.ID;
    }

    @Override
    public UserType decode(UserType type, Map<String, Object> config) {
        super.decode(type,config);
        JSONObject jsonObject = new JSONObject(config);
        ofNullable(jsonObject.getString("property"))
                .ifPresent(type::setProperty);
        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, UserType type) {
        super.doEncode(encoded, type);
        encoded.put("property", type.getProperty());

    }
}
