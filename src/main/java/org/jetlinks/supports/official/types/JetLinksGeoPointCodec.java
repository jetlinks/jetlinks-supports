package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.GeoType;

import java.util.Map;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksGeoPointCodec extends AbstractDataTypeCodec<GeoType> {

    @Override
    public String getTypeId() {
        return GeoType.ID;
    }

    @Override
    public GeoType decode(GeoType type, Map<String, Object> config) {
        super.decode(type, config);
        JSONObject jsonObject = new JSONObject(config);
        ofNullable(jsonObject.getString("latProperty"))
                .ifPresent(type::latProperty);
        ofNullable(jsonObject.getString("lonProperty"))
                .ifPresent(type::lonProperty);
        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, GeoType type) {
        encoded.put("latProperty", type.getLatProperty());
        encoded.put("lonProperty", type.getLonProperty());
    }


}
