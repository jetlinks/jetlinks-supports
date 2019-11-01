package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.DataTypeCodec;
import org.jetlinks.core.metadata.types.DateTimeType;

import java.time.ZoneId;
import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksDateCodec implements DataTypeCodec<DateTimeType> {

    @Override
    public String getTypeId() {
        return DateTimeType.ID;
    }

    @Override
    public DateTimeType decode(DateTimeType type, Map<String, Object> config) {
        JSONObject jsonObject = new JSONObject(config);
        ofNullable(jsonObject.getString("format"))
                .ifPresent(type::setFormat);
        ofNullable(jsonObject.getString("tz"))
                .map(ZoneId::of)
                .ifPresent(type::setZoneId);

        ofNullable(jsonObject.getString("description"))
                .ifPresent(type::setDescription);


        return type;
    }

    @Override
    public Map<String, Object> encode(DateTimeType type) {
        JSONObject json = new JSONObject();
        json.put("format", type.getFormat());
        json.put("tz", type.getZoneId().toString());

        json.put("type",getTypeId());
        json.put("description", type.getDescription());
        return json;
    }
}
