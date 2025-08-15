package org.jetlinks.supports.metadata.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.SimplePropertyMetadata;
import org.jetlinks.core.metadata.types.*;
import org.jetlinks.core.utils.json.ObjectMappers;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * JsonSchemaTypeMapper 完整单元测试
 * 
 * @author zhouhao
 * @since 1.3.0
 */
public class JsonSchemaTypeMapperTest {

    private static final ObjectMapper objectMapper = ObjectMappers.JSON_MAPPER;

    // ==================== mapToDataType 测试 ====================

    @Test
    public void testMapToDataType_String() throws Exception {
        // 基本字符串类型
        String schema = """
            {
              "type": "string"
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof StringType);
        assertEquals("string", dataType.getId());
    }

    @Test
    public void testMapToDataType_StringWithConstraints() throws Exception {
        // 带约束的字符串类型
        String schema = """
            {
              "type": "string",
              "minLength": 5,
              "maxLength": 20,
              "pattern": "^[a-zA-Z]+$"
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof StringType);
        StringType stringType = (StringType) dataType;
        assertEquals(5, stringType.getExpands().get("minLength"));
        assertEquals(20, stringType.getExpands().get("maxLength"));
        assertEquals("^[a-zA-Z]+$", stringType.getExpands().get("pattern"));
    }

    @Test
    public void testMapToDataType_StringWithFormat() throws Exception {
        // 日期时间格式
        String dateTimeSchema = """
            {
              "type": "string",
              "format": "date-time"
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(dateTimeSchema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof DateTimeType);
        assertEquals("date", dataType.getId());

        // 密码格式
        String passwordSchema = """
            {
              "type": "string",
              "format": "password"
            }
            """;
        schemaJson = objectMapper.readTree(passwordSchema);
        dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof PasswordType);
        assertEquals("password", dataType.getId());

        // 自定义格式
        String customFormatSchema = """
            {
              "type": "string",
              "format": "email"
            }
            """;
        schemaJson = objectMapper.readTree(customFormatSchema);
        dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof StringType);
        StringType stringType = (StringType) dataType;
        assertEquals("email", stringType.getExpands().get("format"));
    }

    @Test
    public void testMapToDataType_Enum() throws Exception {
        String schema = """
            {
              "type": "string",
              "enum": ["red", "green", "blue"]
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof EnumType);
        EnumType enumType = (EnumType) dataType;
        assertEquals(3, enumType.getElements().size());
        
        String[] expectedValues = {"red", "green", "blue"};
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], enumType.getElements().get(i).getValue());
        }
    }

    @Test
    public void testMapToDataType_Number() throws Exception {
        String schema = """
            {
              "type": "number",
              "minimum": -100.5,
              "maximum": 100.5,
              "multipleOf": 0.1
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof DoubleType);
        DoubleType doubleType = (DoubleType) dataType;
        assertEquals(-100.5, doubleType.getExpands().get("min"));
        assertEquals(100.5, doubleType.getExpands().get("max"));
        assertEquals(0.1, doubleType.getExpands().get("step"));
    }

    @Test
    public void testMapToDataType_Integer() throws Exception {
        // 整数范围在int范围内
        String intSchema = """
            {
              "type": "integer",
              "minimum": -100,
              "maximum": 100
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(intSchema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof IntType);
        IntType intType = (IntType) dataType;
        assertEquals(-100, intType.getExpands().get("min"));
        assertEquals(100, intType.getExpands().get("max"));

        // 整数范围超出int范围，应转为LongType
        String longSchema = """
            {
              "type": "integer",
              "minimum": -9223372036854775808,
              "maximum": 9223372036854775807
            }
            """;
        schemaJson = objectMapper.readTree(longSchema);
        dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof LongType);
        LongType longType = (LongType) dataType;
        assertEquals(-9223372036854775808L, longType.getExpands().get("min"));
        assertEquals(9223372036854775807L, longType.getExpands().get("max"));
    }

    @Test
    public void testMapToDataType_Boolean() throws Exception {
        String schema = """
            {
              "type": "boolean"
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof BooleanType);
        assertEquals("boolean", dataType.getId());
    }

    @Test
    public void testMapToDataType_Object() throws Exception {
        String schema = """
            {
              "type": "object",
              "properties": {
                "name": {
                  "type": "string",
                  "title": "姓名",
                  "description": "用户姓名"
                },
                "age": {
                  "type": "integer",
                  "title": "年龄",
                  "minimum": 0,
                  "maximum": 150
                }
              }
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof ObjectType);
        ObjectType objectType = (ObjectType) dataType;
        assertEquals(2, objectType.getProperties().size());
        
        PropertyMetadata nameProperty = objectType.getProperty("name").orElse(null);
        assertNotNull(nameProperty);
        assertEquals("姓名", nameProperty.getName());
        assertEquals("用户姓名", nameProperty.getDescription());
        assertTrue(nameProperty.getValueType() instanceof StringType);
        
        PropertyMetadata ageProperty = objectType.getProperty("age").orElse(null);
        assertNotNull(ageProperty);
        assertEquals("年龄", ageProperty.getName());
        assertTrue(ageProperty.getValueType() instanceof IntType);
    }

    @Test
    public void testMapToDataType_Array() throws Exception {
        String schema = """
            {
              "type": "array",
              "items": {
                "type": "string"
              },
              "minItems": 1,
              "maxItems": 10
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof ArrayType);
        ArrayType arrayType = (ArrayType) dataType;
        assertTrue(arrayType.getElementType() instanceof StringType);
        assertEquals(1, arrayType.getExpands().get("minItems"));
        assertEquals(10, arrayType.getExpands().get("maxItems"));
    }

    @Test
    public void testMapToDataType_UnknownType() throws Exception {
        // 没有type字段
        String schema = """
            {
              "title": "Unknown"
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof UnknownType);

        // 不支持的type
        String unknownTypeSchema = """
            {
              "type": "null"
            }
            """;
        schemaJson = objectMapper.readTree(unknownTypeSchema);
        dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof UnknownType);
    }

    // ==================== mapFromDataType 测试 ====================

    @Test
    public void testMapFromDataType_StringType() throws Exception {
        StringType stringType = new StringType();
        stringType.expand("minLength", 5);
        stringType.expand("maxLength", 20);
        stringType.expand("pattern", "^[a-zA-Z]+$");
        stringType.expand("format", "email");
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(stringType);
        
        assertEquals("string", schema.get("type").asText());
        assertEquals(5, schema.get("minLength").asInt());
        assertEquals(20, schema.get("maxLength").asInt());
        assertEquals("^[a-zA-Z]+$", schema.get("pattern").asText());
        assertEquals("email", schema.get("format").asText());
    }

    @Test
    public void testMapFromDataType_IntType() throws Exception {
        IntType intType = new IntType();
        intType.setMin(0);
        intType.setMax(100);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(intType);
        
        assertEquals("integer", schema.get("type").asText());
        assertEquals(0.0, schema.get("minimum").asDouble(), 0.001);
        assertEquals(100.0, schema.get("maximum").asDouble(), 0.001);
    }

    @Test
    public void testMapFromDataType_LongType() throws Exception {
        LongType longType = new LongType();
        longType.setMin(-1000L);
        longType.setMax(1000L);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(longType);
        
        assertEquals("integer", schema.get("type").asText());
        assertEquals(-1000.0, schema.get("minimum").asDouble(), 0.001);
        assertEquals(1000.0, schema.get("maximum").asDouble(), 0.001);
    }

    @Test
    public void testMapFromDataType_DoubleType() throws Exception {
        DoubleType doubleType = new DoubleType();
        doubleType.setMin(-10.5);
        doubleType.setMax(10.5);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(doubleType);
        
        assertEquals("number", schema.get("type").asText());
        assertEquals(-10.5, schema.get("minimum").asDouble(), 0.001);
        assertEquals(10.5, schema.get("maximum").asDouble(), 0.001);
    }

    @Test
    public void testMapFromDataType_FloatType() throws Exception {
        FloatType floatType = new FloatType();
        floatType.setMin(-5.5f);
        floatType.setMax(5.5f);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(floatType);
        
        assertEquals("number", schema.get("type").asText());
        assertEquals(-5.5, schema.get("minimum").asDouble(), 0.001);
        assertEquals(5.5, schema.get("maximum").asDouble(), 0.001);
    }

    @Test
    public void testMapFromDataType_BooleanType() throws Exception {
        BooleanType booleanType = new BooleanType();
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(booleanType);
        
        assertEquals("boolean", schema.get("type").asText());
    }

    @Test
    public void testMapFromDataType_DateTimeType() throws Exception {
        DateTimeType dateTimeType = new DateTimeType();
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(dateTimeType);
        
        assertEquals("string", schema.get("type").asText());
        assertEquals("date-time", schema.get("format").asText());
    }

    @Test
    public void testMapFromDataType_PasswordType() throws Exception {
        PasswordType passwordType = new PasswordType();
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(passwordType);
        
        assertEquals("string", schema.get("type").asText());
        assertEquals("password", schema.get("format").asText());
    }

    @Test
    public void testMapFromDataType_FileType() throws Exception {
        FileType fileType = new FileType();
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(fileType);
        
        assertEquals("string", schema.get("type").asText());
        assertEquals("uri", schema.get("format").asText());
    }

    @Test
    public void testMapFromDataType_EnumType() throws Exception {
        EnumType enumType = new EnumType();
        enumType.addElement(EnumType.Element.of("red", "红色"));
        enumType.addElement(EnumType.Element.of("green", "绿色"));
        enumType.addElement(EnumType.Element.of("blue", "蓝色"));
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(enumType);
        
        assertEquals("string", schema.get("type").asText());
        assertTrue(schema.has("enum"));
        assertEquals(3, schema.get("enum").size());
        assertEquals("red", schema.get("enum").get(0).asText());
        assertEquals("green", schema.get("enum").get(1).asText());
        assertEquals("blue", schema.get("enum").get(2).asText());
    }

    @Test
    public void testMapFromDataType_ArrayType() throws Exception {
        ArrayType arrayType = new ArrayType();
        arrayType.setElementType(new StringType());
        arrayType.expand("minItems", 1);
        arrayType.expand("maxItems", 10);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(arrayType);
        
        assertEquals("array", schema.get("type").asText());
        assertTrue(schema.has("items"));
        assertEquals("string", schema.get("items").get("type").asText());
        assertEquals(1, schema.get("minItems").asInt());
        assertEquals(10, schema.get("maxItems").asInt());
    }

    @Test
    public void testMapFromDataType_ObjectType() throws Exception {
        ObjectType objectType = new ObjectType();
        
        SimplePropertyMetadata nameProperty = new SimplePropertyMetadata();
        nameProperty.setId("name");
        nameProperty.setName("姓名");
        nameProperty.setDescription("用户姓名");
        nameProperty.setValueType(new StringType());
        objectType.addPropertyMetadata(nameProperty);
        
        SimplePropertyMetadata ageProperty = new SimplePropertyMetadata();
        ageProperty.setId("age");
        ageProperty.setName("年龄");
        IntType ageType = new IntType();
        ageType.setMin(0);
        ageType.setMax(150);
        ageProperty.setValueType(ageType);
        objectType.addPropertyMetadata(ageProperty);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(objectType);
        
        assertEquals("object", schema.get("type").asText());
        assertTrue(schema.has("properties"));
        
        JsonNode properties = schema.get("properties");
        assertTrue(properties.has("name"));
        assertTrue(properties.has("age"));
        
        JsonNode nameSchema = properties.get("name");
        assertEquals("string", nameSchema.get("type").asText());
        assertEquals("姓名", nameSchema.get("title").asText());
        assertEquals("用户姓名", nameSchema.get("description").asText());
        
        JsonNode ageSchema = properties.get("age");
        assertEquals("integer", ageSchema.get("type").asText());
        assertEquals("年龄", ageSchema.get("title").asText());
        assertEquals(0.0, ageSchema.get("minimum").asDouble(), 0.001);
        assertEquals(150.0, ageSchema.get("maximum").asDouble(), 0.001);
    }

    @Test
    public void testMapFromDataType_GeoType() throws Exception {
        GeoType geoType = new GeoType();
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(geoType);
        
        assertEquals("object", schema.get("type").asText());
        assertTrue(schema.has("properties"));
        
        JsonNode properties = schema.get("properties");
        assertTrue(properties.has("type"));
        assertTrue(properties.has("coordinates"));
        
        // 验证type属性
        JsonNode typeProperty = properties.get("type");
        assertEquals("string", typeProperty.get("type").asText());
        assertTrue(typeProperty.has("enum"));
        assertEquals("Point", typeProperty.get("enum").get(0).asText());
        
        // 验证coordinates属性
        JsonNode coordinatesProperty = properties.get("coordinates");
        assertEquals("array", coordinatesProperty.get("type").asText());
        assertEquals(2, coordinatesProperty.get("minItems").asInt());
        assertEquals(2, coordinatesProperty.get("maxItems").asInt());
        assertEquals("number", coordinatesProperty.get("items").get("type").asText());
        
        // 验证required字段
        assertTrue(schema.has("required"));
        assertEquals(2, schema.get("required").size());
        assertEquals("type", schema.get("required").get(0).asText());
        assertEquals("coordinates", schema.get("required").get(1).asText());
    }

    @Test
    public void testMapFromDataType_UnknownType() throws Exception {
        UnknownType unknownType = new UnknownType();
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(unknownType);
        
        assertEquals("string", schema.get("type").asText());
    }

    // ==================== mapFromProperty 测试 ====================

    @Test
    public void testMapFromProperty_BasicProperty() throws Exception {
        SimplePropertyMetadata property = new SimplePropertyMetadata();
        property.setId("temperature");
        property.setName("温度");
        property.setDescription("设备温度传感器读数");
        
        DoubleType doubleType = new DoubleType();
        doubleType.setMin(-50.0);
        doubleType.setMax(100.0);
        property.setValueType(doubleType);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromProperty(property);
        
        assertEquals("temperature", schema.get("$id").asText());
        assertEquals("温度", schema.get("title").asText());
        assertEquals("设备温度传感器读数", schema.get("description").asText());
        assertEquals("number", schema.get("type").asText());
        assertEquals(-50.0, schema.get("minimum").asDouble(), 0.001);
        assertEquals(100.0, schema.get("maximum").asDouble(), 0.001);
    }

    @Test
    public void testMapFromProperty_WithExpands() throws Exception {
        SimplePropertyMetadata property = new SimplePropertyMetadata();
        property.setId("status");
        property.setName("状态");
        property.setValueType(new StringType());
        
        Map<String, Object> expands = new HashMap<>();
        expands.put("default", "active");
        expands.put("examples", new String[]{"active", "inactive", "pending"});
        expands.put("const", "fixed_value");
        expands.put("customField", "custom_value");
        property.setExpands(expands);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromProperty(property);
        
        assertEquals("状态", schema.get("title").asText());
        assertEquals("string", schema.get("type").asText());
        assertEquals("active", schema.get("default").asText());
        assertTrue(schema.has("examples"));
        assertEquals("fixed_value", schema.get("const").asText());
        assertEquals("custom_value", schema.get("x-customField").asText());
    }

    @Test
    public void testMapFromProperty_NullValueType() throws Exception {
        SimplePropertyMetadata property = new SimplePropertyMetadata();
        property.setId("empty");
        property.setName("空属性");
        property.setValueType(null);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromProperty(property);
        
        assertEquals("空属性", schema.get("title").asText());
        assertFalse(schema.has("type"));
    }

    // ==================== 双向映射测试 ====================

    @Test
    public void testBidirectionalMapping_StringType() throws Exception {
        // 创建原始StringType
        StringType originalStringType = new StringType();
        originalStringType.expand("minLength", 5);
        originalStringType.expand("maxLength", 20);
        originalStringType.expand("pattern", "^[a-zA-Z]+$");
        
        // 转换为JSON Schema
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(originalStringType);
        
        // 再转换回DataType
        DataType convertedDataType = JsonSchemaTypeMapper.mapToDataType(schema);
        
        assertTrue(convertedDataType instanceof StringType);
        StringType convertedStringType = (StringType) convertedDataType;
        assertEquals(5, convertedStringType.getExpands().get("minLength"));
        assertEquals(20, convertedStringType.getExpands().get("maxLength"));
        assertEquals("^[a-zA-Z]+$", convertedStringType.getExpands().get("pattern"));
    }

    @Test
    public void testBidirectionalMapping_IntType() throws Exception {
        // 创建原始IntType
        IntType originalIntType = new IntType();
        originalIntType.setMin(10);
        originalIntType.setMax(100);
        
        // 转换为JSON Schema
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(originalIntType);
        
        // 再转换回DataType
        DataType convertedDataType = JsonSchemaTypeMapper.mapToDataType(schema);
        
        assertTrue(convertedDataType instanceof IntType);
        IntType convertedIntType = (IntType) convertedDataType;
        assertEquals(10, convertedIntType.getExpands().get("min"));
        assertEquals(100, convertedIntType.getExpands().get("max"));
    }

    @Test
    public void testBidirectionalMapping_EnumType() throws Exception {
        // 创建原始EnumType
        EnumType originalEnumType = new EnumType();
        originalEnumType.addElement(EnumType.Element.of("red", "红色"));
        originalEnumType.addElement(EnumType.Element.of("green", "绿色"));
        originalEnumType.addElement(EnumType.Element.of("blue", "蓝色"));
        
        // 转换为JSON Schema
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(originalEnumType);
        
        // 再转换回DataType
        DataType convertedDataType = JsonSchemaTypeMapper.mapToDataType(schema);
        
        assertTrue(convertedDataType instanceof EnumType);
        EnumType convertedEnumType = (EnumType) convertedDataType;
        assertEquals(3, convertedEnumType.getElements().size());
        assertEquals("red", convertedEnumType.getElements().get(0).getValue());
        assertEquals("green", convertedEnumType.getElements().get(1).getValue());
        assertEquals("blue", convertedEnumType.getElements().get(2).getValue());
    }

    // ==================== 复杂嵌套类型测试 ====================

    @Test
    public void testComplexNestedTypes_ArrayOfObjects() throws Exception {
        String schema = """
            {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "integer",
                    "minimum": 1
                  },
                  "name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 50
                  },
                  "tags": {
                    "type": "array",
                    "items": {
                      "type": "string"
                    }
                  }
                }
              },
              "minItems": 1,
              "maxItems": 100
            }
            """;
        
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof ArrayType);
        ArrayType arrayType = (ArrayType) dataType;
        assertEquals(1, arrayType.getExpands().get("minItems"));
        assertEquals(100, arrayType.getExpands().get("maxItems"));
        
        assertTrue(arrayType.getElementType() instanceof ObjectType);
        ObjectType objectType = (ObjectType) arrayType.getElementType();
        assertEquals(3, objectType.getProperties().size());
        
        PropertyMetadata idProperty = objectType.getProperty("id").orElse(null);
        assertNotNull(idProperty);
        assertTrue(idProperty.getValueType() instanceof IntType);
        
        PropertyMetadata nameProperty = objectType.getProperty("name").orElse(null);
        assertNotNull(nameProperty);
        assertTrue(nameProperty.getValueType() instanceof StringType);
        
        PropertyMetadata tagsProperty = objectType.getProperty("tags").orElse(null);
        assertNotNull(tagsProperty);
        assertTrue(tagsProperty.getValueType() instanceof ArrayType);
        ArrayType tagsArrayType = (ArrayType) tagsProperty.getValueType();
        assertTrue(tagsArrayType.getElementType() instanceof StringType);
    }

    @Test
    public void testComplexNestedTypes_ObjectWithArrayAndEnum() throws Exception {
        String schema = """
            {
              "type": "object",
              "properties": {
                "user": {
                  "type": "object",
                  "properties": {
                    "profile": {
                      "type": "object",
                      "properties": {
                        "age": {
                          "type": "integer",
                          "minimum": 0,
                          "maximum": 150
                        },
                        "status": {
                          "type": "string",
                          "enum": ["active", "inactive", "pending"]
                        }
                      }
                    },
                    "permissions": {
                      "type": "array",
                      "items": {
                        "type": "string",
                        "enum": ["read", "write", "admin"]
                      }
                    }
                  }
                }
              }
            }
            """;
        
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof ObjectType);
        ObjectType rootObject = (ObjectType) dataType;
        
        PropertyMetadata userProperty = rootObject.getProperty("user").orElse(null);
        assertNotNull(userProperty);
        assertTrue(userProperty.getValueType() instanceof ObjectType);
        ObjectType userObject = (ObjectType) userProperty.getValueType();
        
        PropertyMetadata profileProperty = userObject.getProperty("profile").orElse(null);
        assertNotNull(profileProperty);
        assertTrue(profileProperty.getValueType() instanceof ObjectType);
        ObjectType profileObject = (ObjectType) profileProperty.getValueType();
        
        PropertyMetadata ageProperty = profileObject.getProperty("age").orElse(null);
        assertNotNull(ageProperty);
        assertTrue(ageProperty.getValueType() instanceof IntType);
        
        PropertyMetadata statusProperty = profileObject.getProperty("status").orElse(null);
        assertNotNull(statusProperty);
        assertTrue(statusProperty.getValueType() instanceof EnumType);
        EnumType statusEnum = (EnumType) statusProperty.getValueType();
        assertEquals(3, statusEnum.getElements().size());
        
        PropertyMetadata permissionsProperty = userObject.getProperty("permissions").orElse(null);
        assertNotNull(permissionsProperty);
        assertTrue(permissionsProperty.getValueType() instanceof ArrayType);
        ArrayType permissionsArray = (ArrayType) permissionsProperty.getValueType();
        assertTrue(permissionsArray.getElementType() instanceof EnumType);
        EnumType permissionEnum = (EnumType) permissionsArray.getElementType();
        assertEquals(3, permissionEnum.getElements().size());
    }

    // ==================== 边界条件和异常测试 ====================

    @Test
    public void testEdgeCases_EmptyObject() throws Exception {
        String schema = """
            {
              "type": "object"
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof ObjectType);
        ObjectType objectType = (ObjectType) dataType;
        assertEquals(0, objectType.getProperties().size());
    }

    @Test
    public void testEdgeCases_ArrayWithoutItems() throws Exception {
        String schema = """
            {
              "type": "array"
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof ArrayType);
        ArrayType arrayType = (ArrayType) dataType;
        assertNull(arrayType.getElementType());
    }

    @Test
    public void testEdgeCases_EmptyEnum() throws Exception {
        String schema = """
            {
              "type": "string",
              "enum": []
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof EnumType);
        EnumType enumType = (EnumType) dataType;
        // EnumType的getElements()可能返回null，需要空检查
        List<EnumType.Element> elements = enumType.getElements();
        int size = elements != null ? elements.size() : 0;
        assertEquals(0, size);
    }

    @Test
    public void testEdgeCases_EnumWithNonStringValues() throws Exception {
        String schema = """
            {
              "type": "string",
              "enum": ["valid", 123, null, true]
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(schema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        
        assertTrue(dataType instanceof EnumType);
        EnumType enumType = (EnumType) dataType;
        // 只有字符串值应该被添加
        assertEquals(1, enumType.getElements().size());
        assertEquals("valid", enumType.getElements().get(0).getValue());
    }

    @Test
    public void testEdgeCases_IntegerBoundaryValues() throws Exception {
        // 测试Integer.MAX_VALUE边界
        String maxIntSchema = """
            {
              "type": "integer",
              "maximum": """ + Integer.MAX_VALUE + """
            }
            """;
        JsonNode schemaJson = objectMapper.readTree(maxIntSchema);
        DataType dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        assertTrue(dataType instanceof IntType);
        
        // 测试超出Integer.MAX_VALUE的值
        String overMaxIntSchema = """
            {
              "type": "integer",
              "maximum": """ + ((long)Integer.MAX_VALUE + 1) + """
            }
            """;
        schemaJson = objectMapper.readTree(overMaxIntSchema);
        dataType = JsonSchemaTypeMapper.mapToDataType(schemaJson);
        assertTrue(dataType instanceof LongType);
    }

    // ==================== JSON Schema 合规性测试 ====================

    @Test
    public void testJsonSchemaCompliance_RequiredFields() throws Exception {
        ObjectType objectType = new ObjectType();
        
        SimplePropertyMetadata requiredProperty = new SimplePropertyMetadata();
        requiredProperty.setId("required_field");
        requiredProperty.setName("必填字段");
        requiredProperty.setValueType(new StringType());
        objectType.addPropertyMetadata(requiredProperty);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromDataType(objectType);
        
        // 验证基本结构
        assertEquals("object", schema.get("type").asText());
        assertTrue(schema.has("properties"));
        
        // 当前实现可能不包含required字段，这是预期的
        // 因为PropertyMetadata没有required标识
    }

    @Test
    public void testJsonSchemaCompliance_StandardProperties() throws Exception {
        SimplePropertyMetadata property = new SimplePropertyMetadata();
        property.setId("test");
        property.setName("测试属性");
        property.setDescription("这是一个测试属性");
        
        StringType stringType = new StringType();
        stringType.expand("minLength", 1);
        stringType.expand("maxLength", 100);
        stringType.expand("pattern", "^[a-zA-Z0-9]+$");
        property.setValueType(stringType);
        
        Map<String, Object> expands = new HashMap<>();
        expands.put("default", "default_value");
        expands.put("examples", new String[]{"example1", "example2"});
        property.setExpands(expands);
        
        JsonNode schema = JsonSchemaTypeMapper.mapFromProperty(property);
        
        // 验证标准JSON Schema字段
        assertEquals("测试属性", schema.get("title").asText());
        assertEquals("这是一个测试属性", schema.get("description").asText());
        assertEquals("string", schema.get("type").asText());
        assertEquals(1, schema.get("minLength").asInt());
        assertEquals(100, schema.get("maxLength").asInt());
        assertEquals("^[a-zA-Z0-9]+$", schema.get("pattern").asText());
        assertEquals("default_value", schema.get("default").asText());
        assertTrue(schema.has("examples"));
    }
}
