package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.core.metadata.*;
import org.jetlinks.core.metadata.types.DataTypes;
import org.jetlinks.core.metadata.types.UnknownType;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class JetLinksDeviceFunctionMetadata implements FunctionMetadata {

    private transient JSONObject jsonObject;

    private transient FunctionMetadata another;

    @Setter
    private List<PropertyMetadata> inputs;

    @Setter
    private DataType output;

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
    private boolean async;

    @Getter
    @Setter
    private Map<String, Object> expands;

    public JetLinksDeviceFunctionMetadata() {
    }

    public JetLinksDeviceFunctionMetadata(String id, String name, List<PropertyMetadata> inputs, DataType output) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(inputs, "inputs cannot be null");
        this.id = id;
        this.name = name;
        this.inputs = inputs;
        this.output = output;
    }

    public JetLinksDeviceFunctionMetadata(JSONObject jsonObject) {
        fromJson(jsonObject);
    }

    public JetLinksDeviceFunctionMetadata(FunctionMetadata another) {
        this.id = another.getId();
        this.name = another.getName();
        this.description = another.getDescription();
        this.expands = another.getExpands() == null ? null : new HashMap<>(another.getExpands());
        this.another = another;
        this.async = another.isAsync();
    }

    @Override
    public List<PropertyMetadata> getInputs() {
        if (inputs == null && jsonObject != null) {
            inputs = Optional.ofNullable(jsonObject.getJSONArray("inputs"))
                             .map(Collection::stream)
                             .map(stream -> stream
                                 .map(JSONObject.class::cast)
                                 .map(JetLinksPropertyMetadata::new)
                                 .map(PropertyMetadata.class::cast)
                                 .collect(Collectors.toList()))
                             .orElse(Collections.emptyList());
        }
        if (inputs == null && another != null) {
            inputs = another.getInputs()
                            .stream()
                            .map(JetLinksPropertyMetadata::new)
                            .collect(Collectors.toList());
        }
        return inputs;
    }

    @Override
    public DataType getOutput() {
        if (output == null && jsonObject != null) {
            output = Optional
                .ofNullable(jsonObject.getJSONObject("output"))
                .flatMap(conf -> Optional
                    .ofNullable(DataTypes.lookup(conf.getString("type")))
                    .map(Supplier::get)
                    .map(type -> JetLinksDataTypeCodecs.decode(type, conf)))
                .orElseGet(UnknownType::new);
        }
        if (output == null && another != null) {
            output = another.getOutput();
        }
        return output;
    }

    @Override
    public String toString() {
        // /*获取系统信息*/ getSysInfo(Type name,)

        return String.join("", new String[]{
            "/* ", getName(), " */ ",
            output == null ? "void" : output.toString(),
            " ",
            getId(),
            "(",
            String.join(",", getInputs().stream().map(PropertyMetadata::toString).toArray(String[]::new))
            , ")"
        });
    }

    @Override
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);
        json.put("description", description);
        json.put("async", async);
        json.put("inputs", getInputs().stream().map(Jsonable::toJson).collect(Collectors.toList()));
        JetLinksDataTypeCodecs.encode(getOutput())
                              .ifPresent(ot -> json.put("output", ot));
        json.put("expands", expands);

        return json;
    }

    @Override
    public void fromJson(JSONObject json) {
        this.jsonObject = json;
        this.inputs = null;
        this.output = null;
        this.id = json.getString("id");
        this.name = json.getString("name");
        this.description = json.getString("description");
        this.async = json.getBooleanValue("async");
        this.expands = json.getJSONObject("expands");
    }

    @Override
    public FunctionMetadata merge(FunctionMetadata another, MergeOption... option) {
        JetLinksDeviceFunctionMetadata function = new JetLinksDeviceFunctionMetadata(this);
        if (function.expands == null) {
            function.expands = new HashMap<>();
        }
        MergeOption.ExpandsMerge.doWith(DeviceMetadataType.function, another.getExpands(), function.expands, option);

        Map<String, PropertyMetadata> inputs = new LinkedHashMap<>();

        if (CollectionUtils.isNotEmpty(function.getInputs())) {
            for (PropertyMetadata input : function.getInputs()) {
                inputs.put(input.getId(), input);
            }
        }

        if (CollectionUtils.isNotEmpty(another.getInputs())) {
            for (PropertyMetadata input : another.getInputs()) {
                inputs.compute(input.getId(), (k, v) -> {
                    if (v == null) {
                        return input;
                    }
                    if (MergeOption.has(MergeOption.ignoreExists, option)) {
                        return v;
                    }
                    return v.merge(input, option);
                });
            }
        }

        function.inputs = new ArrayList<>(inputs.values());
        return function;
    }
}
