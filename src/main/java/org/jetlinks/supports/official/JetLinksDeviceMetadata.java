package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class JetLinksDeviceMetadata implements DeviceMetadata {

    private JSONObject jsonObject;

    private volatile Map<String, PropertyMetadata> properties;

    private volatile Map<String, FunctionMetadata> functions;

    private volatile Map<String, EventMetadata> events;

    private volatile Map<String, PropertyMetadata> tags;

    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private String name;

    @Getter
    @Setter
    private String description;

    @Setter
    private Map<String, Object> expands;

    public JetLinksDeviceMetadata(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public JetLinksDeviceMetadata(DeviceMetadata another) {
        this.properties = another.getProperties()
                .stream()
                .map(JetLinksPropertyMetadata::new)
                .collect(Collectors.toMap(JetLinksPropertyMetadata::getId, Function.identity(), (a, b) -> a));

        this.functions = another.getFunctions()
                .stream()
                .map(JetLinksDeviceFunctionMetadata::new)
                .collect(Collectors.toMap(JetLinksDeviceFunctionMetadata::getId, Function.identity(), (a, b) -> a));

        this.events = another.getEvents()
                .stream()
                .map(JetLinksEventMetadata::new)
                .collect(Collectors.toMap(JetLinksEventMetadata::getId, Function.identity(), (a, b) -> a));

    }

    @Override
    public List<PropertyMetadata> getProperties() {
        if (properties == null && jsonObject != null) {
            properties = Optional.ofNullable(jsonObject.getJSONArray("properties"))
                    .map(Collection::stream)
                    .map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksPropertyMetadata::new)
                            .map(PropertyMetadata.class::cast)
                            .collect(Collectors.toMap(PropertyMetadata::getId, Function.identity(), (a, b) -> a))
                    )
                    .orElse(Collections.emptyMap());
        }
        if (properties == null) {
            this.properties = new HashMap<>();
        }
        return new ArrayList<>(properties.values());
    }

    @Override
    public List<FunctionMetadata> getFunctions() {
        if (functions == null && jsonObject != null) {
            functions = Optional.ofNullable(jsonObject.getJSONArray("functions"))
                    .map(Collection::stream)
                    .map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksDeviceFunctionMetadata::new)
                            .map(FunctionMetadata.class::cast)
                            .collect(Collectors.toMap(FunctionMetadata::getId, Function.identity(), (a, b) -> a))
                    )
                    .orElse(Collections.emptyMap());
        }
        if (functions == null) {
            this.functions = new HashMap<>();
        }
        return new ArrayList<>(functions.values());
    }

    @Override
    public List<PropertyMetadata> getTags() {
        if (tags == null && jsonObject != null) {
            tags = Optional.ofNullable(jsonObject.getJSONArray("tags"))
                    .map(Collection::stream)
                    .map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksPropertyMetadata::new)
                            .map(PropertyMetadata.class::cast)
                            .collect(Collectors.toMap(PropertyMetadata::getId, Function.identity(), (a, b) -> a))
                    )
                    .orElse(Collections.emptyMap());
        }
        if (tags == null) {
            this.tags = new HashMap<>();
        }
        return new ArrayList<>(tags.values());
    }

    @Override
    public List<EventMetadata> getEvents() {
        if (events == null && jsonObject != null) {
            events = Optional.ofNullable(jsonObject.getJSONArray("events"))
                    .map(Collection::stream)
                    .map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksEventMetadata::new)
                            .map(EventMetadata.class::cast)
                            .collect(Collectors.toMap(EventMetadata::getId, Function.identity(), (a, b) -> a))
                    )
                    .orElse(Collections.emptyMap());
        }
        if (events == null) {
            this.events = new HashMap<>();
        }
        return new ArrayList<>(events.values());
    }

    @Override
    public EventMetadata getEventOrNull(String id) {
        if (events == null) {
            getEvents();
        }
        return events.get(id);
    }

    @Override
    public PropertyMetadata getPropertyOrNull(String id) {
        if (properties == null) {
            getProperties();
        }
        return properties.get(id);
    }

    @Override
    public FunctionMetadata getFunctionOrNull(String id) {
        if (functions == null) {
            getFunctions();
        }
        return functions.get(id);
    }

    @Override
    public PropertyMetadata getTagOrNull(String id) {
        if (tags == null) {
            getTags();
        }
        return tags.get(id);
    }

    public Map<String, Object> getExpands() {
        if (this.expands == null && jsonObject != null) {
            this.expands = jsonObject.getJSONObject("expands");
        }
        return this.expands;
    }

    @Override
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("name", name);
        json.put("description", description);
        json.put("properties", getProperties().stream().map(Jsonable::toJson).collect(Collectors.toList()));
        json.put("functions", getFunctions().stream().map(Jsonable::toJson).collect(Collectors.toList()));
        json.put("events", getEvents().stream().map(Jsonable::toJson).collect(Collectors.toList()));
        json.put("expands", expands);
        return json;
    }

    @Override
    public void fromJson(JSONObject json) {
        this.jsonObject = json;
        this.properties = null;
        this.events = null;
        this.functions = null;
        this.id = json.getString("id");
        this.name = json.getString("name");
        this.description = json.getString("description");
        this.expands = json.getJSONObject("expands");

    }
}
