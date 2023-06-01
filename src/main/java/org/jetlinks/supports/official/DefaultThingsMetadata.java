package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.core.metadata.*;
import org.jetlinks.core.things.ThingMetadata;
import reactor.function.Function3;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author zhouhao
 * @since 1.1.9
 */
public class DefaultThingsMetadata implements ThingMetadata {

    private JSONObject jsonObject;

    private volatile Map<String, PropertyMetadata> properties;

    private volatile Map<String, FunctionMetadata> functions;

    private volatile Map<String, EventMetadata> events;

    private volatile Map<String, PropertyMetadata> tags;

    private volatile List<PropertyMetadata> propertyMetadataList;

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

    public DefaultThingsMetadata(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public DefaultThingsMetadata(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public DefaultThingsMetadata(ThingMetadata another) {
        this.id = another.getId();
        this.name = another.getName();
        this.description = another.getDescription();
        this.expands = another.getExpands();
        this.properties = another.getProperties()
                                 .stream()
                                 .map(JetLinksPropertyMetadata::new)
                                 .collect(Collectors.toMap(JetLinksPropertyMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new));

        this.functions = another.getFunctions()
                                .stream()
                                .map(JetLinksDeviceFunctionMetadata::new)
                                .collect(Collectors.toMap(JetLinksDeviceFunctionMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new));

        this.events = another.getEvents()
                             .stream()
                             .map(JetLinksEventMetadata::new)
                             .collect(Collectors.toMap(JetLinksEventMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new));

        this.tags = another.getTags()
                           .stream()
                           .map(JetLinksPropertyMetadata::new)
                           .collect(Collectors.toMap(JetLinksPropertyMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new));

    }

    @Override
    public List<PropertyMetadata> getProperties() {
        if (CollectionUtils.isNotEmpty(this.propertyMetadataList)) {
            return this.propertyMetadataList;
        }
        if (this.properties == null && jsonObject != null) {
            this.properties = Optional
                    .ofNullable(jsonObject.getJSONArray("properties"))
                    .map(Collection::stream)
                    .<Map<String, PropertyMetadata>>map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksPropertyMetadata::new)
                            .map(PropertyMetadata.class::cast)
                            .collect(Collectors.toMap(PropertyMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new))
                    )
                    .orElse(Collections.emptyMap());
        }

        if (this.propertyMetadataList == null && this.properties != null) {
            this.propertyMetadataList = Collections.unmodifiableList(new ArrayList<>(this.properties.values()));
            return this.propertyMetadataList;
        }
        return Collections.emptyList();
    }

    @Override
    public List<FunctionMetadata> getFunctions() {
        if (functions == null && jsonObject != null) {
            functions = Optional
                    .ofNullable(jsonObject.getJSONArray("functions"))
                    .map(Collection::stream)
                    .<Map<String, FunctionMetadata>>map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksDeviceFunctionMetadata::new)
                            .map(FunctionMetadata.class::cast)
                            .collect(Collectors.toMap(FunctionMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new))
                    )
                    .orElse(Collections.emptyMap());
        }
        if (functions == null) {
            this.functions = new LinkedHashMap<>();
        }
        return new ArrayList<>(functions.values());
    }

    @Override
    public List<PropertyMetadata> getTags() {
        if (tags == null && jsonObject != null) {
            tags = Optional
                    .ofNullable(jsonObject.getJSONArray("tags"))
                    .map(Collection::stream)
                    .<Map<String, PropertyMetadata>>map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksPropertyMetadata::new)
                            .map(PropertyMetadata.class::cast)
                            .collect(Collectors.toMap(PropertyMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new))
                    )
                    .orElse(Collections.emptyMap());
        }
        if (tags == null) {
            this.tags = new LinkedHashMap<>();
        }
        return new ArrayList<>(tags.values());
    }

    @Override
    public List<EventMetadata> getEvents() {
        if (events == null && jsonObject != null) {
            events = Optional
                    .ofNullable(jsonObject.getJSONArray("events"))
                    .map(Collection::stream)
                    .<Map<String, EventMetadata>>map(stream -> stream
                            .map(JSONObject.class::cast)
                            .map(JetLinksEventMetadata::new)
                            .map(EventMetadata.class::cast)
                            .collect(Collectors.toMap(EventMetadata::getId, Function.identity(), (a, b) -> a, LinkedHashMap::new))
                    )
                    .orElse(Collections.emptyMap());
        }
        if (events == null) {
            this.events = new LinkedHashMap<>();
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
        return properties == null ? null : properties.get(id);
    }

    @Override
    public FunctionMetadata getFunctionOrNull(String id) {
        if (functions == null) {
            getFunctions();
        }
        return functions == null ? null : functions.get(id);
    }

    @Override
    public PropertyMetadata getTagOrNull(String id) {
        if (tags == null) {
            getTags();
        }
        return tags == null ? null : tags.get(id);
    }

    @Override
    public PropertyMetadata findProperty(Predicate<PropertyMetadata> predicate) {
        for (PropertyMetadata metadata : getProperties()) {
            if (predicate.test(metadata)) {
                return metadata;
            }
        }
        return null;
    }

    public void addProperty(PropertyMetadata metadata) {
        if (this.properties == null) {
            this.properties = new LinkedHashMap<>();
        }
        this.properties.put(metadata.getId(), new JetLinksPropertyMetadata(metadata));
        this.propertyMetadataList = Collections.unmodifiableList(new ArrayList<>(this.properties.values()));
    }

    public void addFunction(FunctionMetadata metadata) {
        if (this.functions == null) {
            this.functions = new LinkedHashMap<>();
        }
        this.functions.put(metadata.getId(), new JetLinksDeviceFunctionMetadata(metadata));
    }

    public void addEvent(EventMetadata metadata) {
        if (this.events == null) {
            this.events = new LinkedHashMap<>();
        }
        this.events.put(metadata.getId(), new JetLinksEventMetadata(metadata));
    }

    public void addTag(PropertyMetadata metadata) {
        if (this.tags == null) {
            this.tags = new LinkedHashMap<>();
        }
        this.tags.put(metadata.getId(), new JetLinksPropertyMetadata(metadata));
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
        json.put("tags", getTags().stream().map(Jsonable::toJson).collect(Collectors.toList()));
        json.put("expands", expands);
        return json;
    }

    @Override
    public void fromJson(JSONObject json) {
        this.jsonObject = json;
        this.properties = null;
        this.events = null;
        this.functions = null;
        this.tags = null;
        this.id = json.getString("id");
        this.name = json.getString("name");
        this.description = json.getString("description");
        this.expands = json.getJSONObject("expands");

    }

    private <V extends Metadata> void doMerge(Map<String, V> map,
                                              V value,
                                              Function3<V, V, MergeOption[], V> mergeFunction,
                                              MergeOption... options) {

        map.compute(value.getId(), (k, old) -> {
            if (old == null) {
                return value;
            }
            //忽略已存在的物模型
            if (MergeOption.has(MergeOption.ignoreExists, options)) {
                return old;
            }
            return mergeFunction.apply(old, value, options);
        });

    }

    protected DefaultThingsMetadata copy() {
        return new DefaultThingsMetadata(this);
    }

    @Override
    public <T extends ThingMetadata> ThingMetadata merge(T metadata, MergeOption... options) {
        DefaultThingsMetadata deviceMetadata = copy();

        if (MergeOption.has(MergeOption.overwriteProperty, options)) {
            deviceMetadata.properties.clear();
        }

        for (PropertyMetadata property : metadata.getProperties()) {
            doMerge(deviceMetadata.properties, property, PropertyMetadata::merge, options);
        }
        //属性过滤
        if (MergeOption.PropertyFilter.has(options)) {
            Map<String, PropertyMetadata> temp = new LinkedHashMap<>(deviceMetadata.properties);
            deviceMetadata.properties.clear();
            for (Map.Entry<String, PropertyMetadata> entry : temp.entrySet()) {
                if (MergeOption.PropertyFilter.doFilter(entry.getValue(), options)) {
                    deviceMetadata.properties.put(entry.getKey(), entry.getValue());
                }
            }
        }

        for (FunctionMetadata func : metadata.getFunctions()) {
            doMerge(deviceMetadata.functions, func, FunctionMetadata::merge, options);
        }

        for (EventMetadata event : metadata.getEvents()) {
            doMerge(deviceMetadata.events, event, EventMetadata::merge, options);
        }

        for (PropertyMetadata tag : metadata.getTags()) {
            doMerge(deviceMetadata.tags, tag, PropertyMetadata::merge, options);
        }

        return deviceMetadata;
    }
}
