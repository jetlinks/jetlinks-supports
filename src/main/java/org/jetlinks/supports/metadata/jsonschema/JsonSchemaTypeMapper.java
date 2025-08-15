package org.jetlinks.supports.metadata.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.SimplePropertyMetadata;
import org.jetlinks.core.metadata.types.*;
import org.jetlinks.core.utils.json.ObjectMappers;

import java.util.List;
import java.util.Map;

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

        return switch (type) {
            case "string" -> mapStringType(schemaJson);
            case "number" -> mapNumberType(schemaJson);
            case "integer" -> mapIntegerType(schemaJson);
            case "boolean" -> new BooleanType();
            case "object" -> mapObjectType(schemaJson);
            case "array" -> mapArrayType(schemaJson);
            default -> new UnknownType();
        };
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

        // 处理枚举
        if (schemaJson.has("enum")) {
            JsonNode enumArray = schemaJson.get("enum");
            EnumType enumType = new EnumType();
            for (JsonNode enumValue : enumArray) {
                if (enumValue.isTextual()) {
                    String value = enumValue.asText();
                    enumType.addElement(EnumType.Element.of(value, value));
                }
            }
            return enumType;
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
                    DataType propertyType = mapToDataType(propertySchema);

                    SimplePropertyMetadata propertyMetadata = new SimplePropertyMetadata();
                    propertyMetadata.setId(propertyName);
                    propertyMetadata.setValueType(propertyType);

                    if (propertySchema.has("title")) {
                        propertyMetadata.setName(propertySchema.get("title").asText());
                    } else {
                        propertyMetadata.setName(propertyName);
                    }

                    if (propertySchema.has("description")) {
                        propertyMetadata.setDescription(propertySchema.get("description").asText());
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
            default:
                schema.put("type", "string");
                break;
        }
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
            if (numberType.getMax() != null) {
                schema.put("maximum", numberType.getMax().doubleValue());
            }
            if (numberType.getMin() != null) {
                schema.put("minimum", numberType.getMin().doubleValue());
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
        schema.put("type", "string");
        ArrayNode enumArray = OBJECT_MAPPER.createArrayNode();

        List<EnumType.Element> elements = enumType.getElements();
        if (elements != null) {
            for (EnumType.Element element : elements) {
                enumArray.add(element.getValue().toString());
            }
        }

        if (enumArray.size() > 0) {
            schema.set("enum", enumArray);
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
            schema.putPOJO("default", expands.get("default"));
        }
        if (expands.containsKey("examples")) {
            schema.putPOJO("examples", expands.get("examples"));
        }
        if (expands.containsKey("const")) {
            schema.putPOJO("const", expands.get("const"));
        }

        // 其他扩展属性作为自定义字段
        for (Map.Entry<String, Object> entry : expands.entrySet()) {
            String key = entry.getKey();
            if (!isStandardJsonSchemaProperty(key)) {
                schema.putPOJO("x-" + key, entry.getValue());
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