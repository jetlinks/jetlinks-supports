package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.DataTypeCodec;
import org.jetlinks.core.metadata.types.StringType;

import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksStringCodec implements DataTypeCodec<StringType> {

    @Override
    public String getTypeId() {
        return StringType.ID;
    }

    @Override
    public StringType decode(StringType type, Map<String, Object> config) {
        ofNullable(config.get("description"))
                .map(String::valueOf)
                .ifPresent(type::setDescription);

        return type;
    }

    @Override
    public Map<String, Object> encode(StringType type) {
        JSONObject json = new JSONObject();
        json.put("type", getTypeId());
        json.put("description", type.getDescription());
        return json;
    }
}
