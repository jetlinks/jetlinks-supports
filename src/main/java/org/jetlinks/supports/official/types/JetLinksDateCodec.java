package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.DateTimeType;

import java.time.ZoneId;
import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksDateCodec extends AbstractDataTypeCodec<DateTimeType> {

    @Override
    public String getTypeId() {
        return DateTimeType.ID;
    }

    @Override
    public DateTimeType decode(DateTimeType type, Map<String, Object> config) {
        super.decode(type,config);
        JSONObject jsonObject = new JSONObject(config);
        ofNullable(jsonObject.getString("format"))
                .ifPresent(type::setFormat);
        ofNullable(jsonObject.getString("tz"))
                .map(ZoneId::of)
                .ifPresent(type::setZoneId);


        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, DateTimeType type) {
        super.doEncode(encoded, type);
        encoded.put("format", type.getFormat());
        encoded.put("tz", type.getZoneId().toString());

    }
}
