package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.DeviceMetadataType;
import org.jetlinks.core.metadata.EventMetadata;
import org.jetlinks.core.metadata.MergeOption;
import org.jetlinks.core.metadata.MetadataDefinitionParser;
import org.jetlinks.core.metadata.types.UnknownType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 默认事件声明，惰性读取载荷类型；未声明或无法识别类型时使用 UnknownType。
 * 实例只保存模型描述或复制来源，不订阅事件或加载设备对象。
 *
 * @author zhouhao
 * @since 1.0.0
 */
public class JetLinksEventMetadata implements EventMetadata {

    private JSONObject jsonObject;

    private DataType type;

    private transient EventMetadata another;

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

    public JetLinksEventMetadata(String id, String name, DataType type) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(type, "type cannot be null");
        this.id = id;
        this.name = name;
        this.type = type;
    }

    public JetLinksEventMetadata(JSONObject jsonObject) {
        fromJson(jsonObject);
    }

    public JetLinksEventMetadata(EventMetadata another) {
        this.another = another;
        this.id = another.getId();
        this.name = another.getName();
        this.description = another.getDescription();
        this.expands = another.getExpands();
    }

    @Override
    public DataType getType() {
        if (type == null && jsonObject != null) {
            Object definition = jsonObject.get("valueType");
            type = JetLinksDataTypeCodecs
                .decode(definition == null ? null : MetadataDefinitionParser.parseObject(definition))
                .orElseGet(UnknownType::new);
        }
        if (type == null && another != null) {
            type = another.getType();
        }
        return type == null ? UnknownType.GLOBAL : type;
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", id);
        jsonObject.put("name", name);
        jsonObject.put("description", description);
        jsonObject.put("valueType", JetLinksDataTypeCodecs.encode(getType()).orElse(null));
        jsonObject.put("expands", expands);
        return jsonObject;
    }

    @Override
    public void fromJson(JSONObject json) {
        this.jsonObject = json;
        this.type = null;
        this.id = json.getString("id");
        this.name = json.getString("name");
        this.description = json.getString("description");
        this.expands = json.getJSONObject("expands");

    }

    @Override
    public EventMetadata merge(EventMetadata another, MergeOption... option) {
        JetLinksEventMetadata metadata = new JetLinksEventMetadata(this);
        if (metadata.expands == null) {
            metadata.expands = new HashMap<>();
        }

        MergeOption.ExpandsMerge.doWith(DeviceMetadataType.event, another.getExpands(), metadata.expands, option);

        return metadata;
    }
}
