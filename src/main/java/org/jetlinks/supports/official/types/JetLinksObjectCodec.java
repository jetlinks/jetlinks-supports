package org.jetlinks.supports.official.types;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.supports.official.JetLinksPropertyMetadata;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

@Getter
@Setter
public class JetLinksObjectCodec extends AbstractDataTypeCodec<ObjectType> {

    @Override
    public String getTypeId() {
        return ObjectType.ID;
    }

    @Override
    public ObjectType decode(ObjectType type, Map<String, Object> config) {
        super.decode(type,config);
        JSONObject jsonObject = new JSONObject(config);

        ofNullable(jsonObject.getJSONArray("properties"))
                .map(list -> list
                        .stream()
                        .map(JSONObject.class::cast)
                        .<PropertyMetadata>map(JetLinksPropertyMetadata::new)
                        .collect(Collectors.toList()))
                .ifPresent(type::setProperties);


        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, ObjectType type) {
        super.doEncode(encoded,type);
        encoded.put("properties", type.getProperties()
                .stream()
                .map(PropertyMetadata::toJson)
                .collect(Collectors.toList()));
    }
}
