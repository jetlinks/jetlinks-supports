package org.jetlinks.supports.metadata.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.Converter;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.SimplePropertyMetadata;
import org.jetlinks.core.metadata.types.*;
import org.jetlinks.core.metadata.unit.ValueUnits;
import org.jetlinks.core.utils.json.ObjectMappers;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * JetLinks DataType 与 JSON Schema 类型映射器
 * 基于Jackson严格按照 JSON Schema Draft 7 规范进行映射
 *
 * @author zhouhao
 * @since 1.3.1
 */
public class JsonSchemaTypeMapper {

    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.JSON_MAPPER;

    /**
     * 将 JSON Schema 映射为 JetLinks DataType
     */
    public static DataType mapToDataType(JsonNode schemaJson) {
        if (!schemaJson.has("type")) {
            return new UnknownType();
        }

        String type = schemaJson.get("type").asText();

        DataType result = schemaJson.has("enum") ? mapEnumType(schemaJson) : switch (type) {
            case "string" -> mapStringType(schemaJson);
            case "number" -> mapNumberType(schemaJson);
            case "integer" -> mapIntegerType(schemaJson);
            case "boolean" -> new BooleanType();
            case "object" -> mapObjectType(schemaJson);
            case "array" -> mapArrayType(schemaJson);
            default -> new UnknownType();
        };
        readExpands(schemaJson).forEach(result::expand);
        if (result instanceof NumberType<?> number) {
            if (schemaJson.has("x-unit")) {
                ValueUnits.lookup(schemaJson.get("x-unit").asText()).ifPresent(number::setUnit);
            }
        }
        return result;
    }

    /** 还原字段名称和显式扩展；扩展沿用输出端的 x- 前缀，不推断语义。 */
    public static SimplePropertyMetadata mapToProperty(JsonNode schema) {
        SimplePropertyMetadata property = new SimplePropertyMetadata();
        if (schema.has("$id")) property.setId(schema.get("$id").asText());
        if (schema.has("title")) property.setName(schema.get("title").asText());
        if (schema.has("description")) property.setDescription(schema.get("description").asText());
        property.setValueType(mapToDataType(schema));
        property.setExpands(readExpands(schema));
        return property;
    }

    private static Map<String, Object> readExpands(JsonNode schema) {
        Map<String, Object> expands = new LinkedHashMap<>();
        schema.fields().forEachRemaining(entry -> {
            if (entry.getKey().startsWith("x-")) {
                expands.put(entry.getKey().substring(2), OBJECT_MAPPER.convertValue(entry.getValue(), Object.class));
            }
        });
        return expands;
    }

    private static EnumType mapEnumType(JsonNode schema) {
        EnumType result = new EnumType();
        String type = schema.path("type").asText();
        result.setValueType(switch (type) {
            case "integer" -> mapIntegerType(schema);
            case "number" -> mapNumberType(schema);
            case "boolean" -> new BooleanType();
            default -> new StringType();
        });
        int index = 0;
        for (JsonNode value : schema.get("enum")) {
            boolean compatible = switch (type) {
                case "integer" -> value.isIntegralNumber();
                case "number" -> value.isNumber();
                case "boolean" -> value.isBoolean();
                default -> value.isTextual();
            };
            if (compatible) {
                Object code = OBJECT_MAPPER.convertValue(value, Object.class);
                String label = schema.path("enumNames").path(index).asText(value.asText());
                result.addElement(EnumType.Element.of(code.toString(), label));
            }
            index++;
        }
        return result;
    }

    public static JsonNode mapFromDataType(DataType dataType) {
        ObjectNode schema = OBJECT_MAPPER.createObjectNode();
        // 映射数据类型
        mapDataTypeToSchema(dataType, schema);

        return schema;
    }

    /**
     * 将 JetLinks PropertyMetadata 映射为 JSON Schema
     */
    public static JsonNode mapFromProperty(PropertyMetadata propertyMetadata) {
        ObjectNode schema = OBJECT_MAPPER.createObjectNode();
        if (propertyMetadata.getId() != null) {
            schema.put("$id", propertyMetadata.getId());
        }
        // 基本信息
        if (propertyMetadata.getName() != null) {
            schema.put("title", propertyMetadata.getName());
        }
        if (propertyMetadata.getDescription() != null) {
            schema.put("description", propertyMetadata.getDescription());
        }

        DataType dataType = propertyMetadata.getValueType();
        if (dataType == null) {
            return schema;
        }

        // 映射数据类型
        mapDataTypeToSchema(dataType, schema);

        // 处理扩展属性
        Map<String, Object> expands = propertyMetadata.getExpands();
        if (expands != null && !expands.isEmpty()) {
            mapExpandsToSchema(expands, schema);
        }

        return schema;
    }

    private static DataType mapStringType(JsonNode schemaJson) {
        // 处理字符串格式
        if (schemaJson.has("format")) {
            String format = schemaJson.get("format").asText();
            switch (format) {
                case "date-time":
                    return new DateTimeType();
                case "password":
                    return new PasswordType();
                default:
                    StringType stringType = new StringType();
                    stringType.expand("format", format);
                    return applyStringConstraints(stringType, schemaJson);
            }
        }

        StringType stringType = new StringType();
        return applyStringConstraints(stringType, schemaJson);
    }

    private static StringType applyStringConstraints(StringType stringType, JsonNode schemaJson) {
        // 处理字符串约束
        if (schemaJson.has("minLength")) {
            stringType.expand("minLength", schemaJson.get("minLength").asInt());
        }
        if (schemaJson.has("maxLength")) {
            stringType.expand("maxLength", schemaJson.get("maxLength").asInt());
        }
        if (schemaJson.has("pattern")) {
            stringType.expand("pattern", schemaJson.get("pattern").asText());
        }

        return stringType;
    }

    private static DataType mapNumberType(JsonNode schemaJson) {
        DoubleType doubleType = new DoubleType();

        if (schemaJson.has("minimum")) {
            doubleType.expand("min", schemaJson.get("minimum").asDouble());
        }
        if (schemaJson.has("maximum")) {
            doubleType.expand("max", schemaJson.get("maximum").asDouble());
        }
        if (schemaJson.has("multipleOf")) {
            doubleType.expand("step", schemaJson.get("multipleOf").asDouble());
        }

        return doubleType;
    }

    private static DataType mapIntegerType(JsonNode schemaJson) {
        if ("int64".equals(schemaJson.path("format").asText())) {
            LongType type = new LongType();
            if (schemaJson.has("minimum")) type.expand("min", schemaJson.get("minimum").asLong());
            if (schemaJson.has("maximum")) type.expand("max", schemaJson.get("maximum").asLong());
            return type;
        }
        IntType intType = new IntType();

        if (schemaJson.has("minimum")) {
            long min = schemaJson.get("minimum").asLong();
            if (min >= Integer.MIN_VALUE && min <= Integer.MAX_VALUE) {
                intType.expand("min", (int) min);
            } else {
                LongType longType = new LongType();
                longType.expand("min", min);
                if (schemaJson.has("maximum")) {
                    longType.expand("max", schemaJson.get("maximum").asLong());
                }
                return longType;
            }
        }
        if (schemaJson.has("maximum")) {
            long max = schemaJson.get("maximum").asLong();
            if (max >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE) {
                intType.expand("max", (int) max);
            } else {
                LongType longType = new LongType();
                longType.expand("max", max);
                if (schemaJson.has("minimum")) {
                    longType.expand("min", schemaJson.get("minimum").asLong());
                }
                return longType;
            }
        }

        return intType;
    }

    private static ObjectType mapObjectType(JsonNode schemaJson) {
        ObjectType objectType = new ObjectType();

        if (schemaJson.has("properties")) {
            JsonNode properties = schemaJson.get("properties");
            properties
                .fieldNames()
                .forEachRemaining(propertyName -> {
                    JsonNode propertySchema = properties.get(propertyName);
                    SimplePropertyMetadata propertyMetadata = mapToProperty(propertySchema);
                    propertyMetadata.setId(propertyName);

                    if (propertyMetadata.getName() == null) {
                        propertyMetadata.setName(propertyName);
                    }

                    objectType.addPropertyMetadata(propertyMetadata);
                });
        }

        return objectType;
    }

    private static ArrayType mapArrayType(JsonNode schemaJson) {
        ArrayType arrayType = new ArrayType();

        if (schemaJson.has("items")) {
            JsonNode items = schemaJson.get("items");
            if (items.isObject()) {
                DataType elementType = mapToDataType(items);
                arrayType.setElementType(elementType);
            }
        }

        if (schemaJson.has("minItems")) {
            arrayType.expand("minItems", schemaJson.get("minItems").asInt());
        }
        if (schemaJson.has("maxItems")) {
            arrayType.expand("maxItems", schemaJson.get("maxItems").asInt());
        }

        return arrayType;
    }

    private static void mapDataTypeToSchema(DataType dataType, ObjectNode schema) {
        String typeId = dataType.getId();

        switch (typeId) {
            case StringType.ID:
                schema.put("type", "string");
                mapStringTypeToSchema((StringType) dataType, schema);
                break;
            case "int":
            case "long":
            case "short":
            case "byte":
                schema.put("type", "integer");
                if ("long".equals(typeId)) schema.put("format", "int64");
                mapNumberTypeToSchema(dataType, schema);
                break;
            case "double":
            case "float":
                schema.put("type", "number");
                mapNumberTypeToSchema(dataType, schema);
                break;
            case BooleanType.ID:
                schema.put("type", "boolean");
                break;
            case ObjectType.ID:
                schema.put("type", "object");
                mapObjectTypeToSchema((ObjectType) dataType, schema);
                break;
            case ArrayType.ID:
                schema.put("type", "array");
                mapArrayTypeToSchema((ArrayType) dataType, schema);
                break;
            case EnumType.ID:
                mapEnumTypeToSchema((EnumType) dataType, schema);
                break;
            case DateTimeType.ID:
                schema.put("type", "string");
                schema.put("format", "date-time");
                break;
            case PasswordType.ID:
                schema.put("type", "string");
                schema.put("format", "password");
                break;
            case FileType.ID:
                schema.put("type", "string");
                schema.put("format", "uri");
                break;
            case GeoType.ID:
                mapGeoTypeToSchema(schema);
                break;
            case UnknownType.ID:
                break;
            default:
                schema.put("type", "string");
                break;
        }
        if (dataType.getExpands() != null) mapExpandsToSchema(dataType.getExpands(), schema);
    }

    private static void mapStringTypeToSchema(StringType stringType, ObjectNode schema) {
        Map<String, Object> expands = stringType.getExpands();
        if (expands != null) {
            if (expands.containsKey("minLength")) {
                schema.put("minLength", (Integer) expands.get("minLength"));
            }
            if (expands.containsKey("maxLength")) {
                schema.put("maxLength", (Integer) expands.get("maxLength"));
            }
            if (expands.containsKey("pattern")) {
                schema.put("pattern", (String) expands.get("pattern"));
            }
            if (expands.containsKey("format")) {
                schema.put("format", (String) expands.get("format"));
            }
        }
    }

    private static void mapNumberTypeToSchema(DataType dataType, ObjectNode schema) {
        if (dataType instanceof NumberType<?> numberType) {
            if (numberType.getUnit() != null) schema.put("x-unit", numberType.getUnit().getId());
            if (numberType.getMax() != null) {
                schema.set("maximum", OBJECT_MAPPER.valueToTree(numberType.getMax()));
            }
            if (numberType.getMin() != null) {
                schema.set("minimum", OBJECT_MAPPER.valueToTree(numberType.getMin()));
            }
        }
    }

    private static void mapObjectTypeToSchema(ObjectType objectType, ObjectNode schema) {
        ObjectNode properties = OBJECT_MAPPER.createObjectNode();
        ArrayNode required = OBJECT_MAPPER.createArrayNode();

        List<PropertyMetadata> propertyList = objectType.getProperties();
        if (propertyList != null) {
            for (PropertyMetadata property : propertyList) {
                JsonNode propertySchema = mapFromProperty(property);
                properties.set(property.getId(), propertySchema);
            }
        }

        schema.set("properties", properties);
        if (required.size() > 0) {
            schema.set("required", required);
        }
    }

    private static void mapArrayTypeToSchema(ArrayType arrayType, ObjectNode schema) {
        DataType elementType = arrayType.getElementType();
        if (elementType != null) {
            SimplePropertyMetadata elementMetadata = new SimplePropertyMetadata();
            elementMetadata.setValueType(elementType);
            JsonNode itemsSchema = mapFromProperty(elementMetadata);
            schema.set("items", itemsSchema);
        }

        Map<String, Object> expands = arrayType.getExpands();
        if (expands != null) {
            if (expands.containsKey("minItems")) {
                schema.put("minItems", (Integer) expands.get("minItems"));
            }
            if (expands.containsKey("maxItems")) {
                schema.put("maxItems", (Integer) expands.get("maxItems"));
            }
        }
    }

    private static void mapEnumTypeToSchema(EnumType enumType, ObjectNode schema) {
        DataType valueType = enumType.getValueType();
        mapDataTypeToSchema(valueType == null ? new StringType() : valueType, schema);
        ArrayNode enumArray = OBJECT_MAPPER.createArrayNode();
        ArrayNode names = OBJECT_MAPPER.createArrayNode();

        List<EnumType.Element> elements = enumType.getElements();
        if (elements != null) {
            for (EnumType.Element element : elements) {
                Object value = element.getValue();
                if (valueType instanceof Converter<?> converter) value = converter.convert(value);
                enumArray.add(OBJECT_MAPPER.valueToTree(value));
                names.add(element.getText());
            }
        }

        schema.set("enum", enumArray);
        if (names.size() > 0) {
            schema.set("enumNames", names);
        }
    }

    private static void mapGeoTypeToSchema(ObjectNode schema) {
        // GeoJSON Point格式
        schema.put("type", "object");
        ObjectNode properties = OBJECT_MAPPER.createObjectNode();

        // type属性
        ObjectNode typeProperty = OBJECT_MAPPER.createObjectNode();
        typeProperty.put("type", "string");
        ArrayNode enumArray = OBJECT_MAPPER.createArrayNode();
        enumArray.add("Point");
        typeProperty.set("enum", enumArray);
        properties.set("type", typeProperty);

        // coordinates属性
        ObjectNode coordinatesProperty = OBJECT_MAPPER.createObjectNode();
        coordinatesProperty.put("type", "array");
        coordinatesProperty.put("minItems", 2);
        coordinatesProperty.put("maxItems", 2);
        ObjectNode numberSchema = OBJECT_MAPPER.createObjectNode();
        numberSchema.put("type", "number");
        coordinatesProperty.set("items", numberSchema);
        properties.set("coordinates", coordinatesProperty);

        schema.set("properties", properties);
        ArrayNode required = OBJECT_MAPPER.createArrayNode();
        required.add("type");
        required.add("coordinates");
        schema.set("required", required);
    }

    private static void mapExpandsToSchema(Map<String, Object> expands, ObjectNode schema) {
        // 映射一些通用的扩展属性
        if (expands.containsKey("default")) {
            schema.set("default", OBJECT_MAPPER.valueToTree(expands.get("default")));
        }
        if (expands.containsKey("examples")) {
            schema.set("examples", OBJECT_MAPPER.valueToTree(expands.get("examples")));
        }
        if (expands.containsKey("const")) {
            schema.set("const", OBJECT_MAPPER.valueToTree(expands.get("const")));
        }

        // 其他扩展属性作为自定义字段
        for (Map.Entry<String, Object> entry : expands.entrySet()) {
            String key = entry.getKey();
            if (!isStandardJsonSchemaProperty(key)) {
                schema.set("x-" + key, OBJECT_MAPPER.valueToTree(entry.getValue()));
            }
        }
    }

    private static boolean isStandardJsonSchemaProperty(String key) {
        return switch (key) {
            case "type", "properties", "items", "required", "enum", "const", "default", "examples", "title",
                 "description", "minimum", "maximum", "multipleOf", "minLength", "maxLength", "pattern", "format",
                 "minItems", "maxItems" -> true;
            default -> false;
        };
    }
}
