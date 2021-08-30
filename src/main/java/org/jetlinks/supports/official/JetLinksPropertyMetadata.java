package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.*;
import org.jetlinks.core.metadata.types.DataTypes;
import org.jetlinks.core.metadata.types.UnknownType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class JetLinksPropertyMetadata implements PropertyMetadata {

    private JSONObject json;

    @Setter
    private transient DataType dataType;

    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private String name;

    @Getter
    @Setter
    private String description;

    @Getter
    @Setter
    private Map<String, Object> expands;

    public JetLinksPropertyMetadata() {

    }

    public JetLinksPropertyMetadata(String id, String name, DataType type) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(type, "type cannot be null");
        this.id = id;
        this.name = name;
        this.dataType = type;
    }

    public JetLinksPropertyMetadata(JSONObject json) {
        fromJson(json);
    }

    public JetLinksPropertyMetadata(PropertyMetadata another) {
        this.id = another.getId();
        this.name = another.getName();
        this.description = another.getDescription();
        this.dataType = another.getValueType();
        this.expands = another.getExpands() == null ? null : new HashMap<>(another.getExpands());
    }

    protected Optional<DataTypeCodec<DataType>> getDataTypeCodec(DataType dataType) {

        return JetLinksDataTypeCodecs.getCodec(dataType.getId());
    }

    protected DataType parseDataType() {
        JSONObject dataTypeJson = json.getJSONObject("valueType");
        if (dataTypeJson == null) {
            throw new IllegalArgumentException("属性" + getId() + "类型不能为空");
        }
        DataType dataType = Optional
                .ofNullable(dataTypeJson.getString("type"))
                .map(DataTypes::lookup)
                .map(Supplier::get)
                .orElseGet(UnknownType::new);

        getDataTypeCodec(dataType)
                .ifPresent(codec -> codec.decode(dataType, dataTypeJson));

        return dataType;
    }

    @Override
    public DataType getValueType() {
        if (dataType == null && json != null) {
            dataType = parseDataType();
        }
        return dataType;
    }


    @Override
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);
        json.put("description", description);
        json.put("valueType", JetLinksDataTypeCodecs.encode(getValueType()).orElse(null));
        json.put("expands", expands);
        return json;
    }

    @Override
    public void fromJson(JSONObject jsonObject) {
        Objects.requireNonNull(jsonObject);
        this.json = jsonObject;
        this.id = json.getString("id");
        this.name = json.getString("name");
        this.description = json.getString("description");
        this.dataType = null;
        this.expands = json.getJSONObject("expands");

    }

    @Override
    public String toString() {
        //  /* 测试 */ int name,
        return String.join("",
                           getValueType().getId(), " ", getId(), " /* ", getName(), " */ "
        );

    }

    @Override
    public PropertyMetadata merge(PropertyMetadata another, MergeOption... option) {
        JetLinksPropertyMetadata metadata = new JetLinksPropertyMetadata(this);
        if (metadata.expands == null) {
            metadata.expands = new HashMap<>();
        }

        MergeOption.ExpandsMerge.doWith(DeviceMetadataType.property, another.getExpands(), metadata.expands, option);

        return metadata;
    }
}
