package org.jetlinks.supports.official;

import org.hswebframework.web.exception.I18nSupportException;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.DataTypeCodec;
import org.jetlinks.core.metadata.MetadataDefinitionParser;
import org.jetlinks.core.metadata.types.DataTypes;
import org.jetlinks.supports.official.types.*;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 默认物模型的数据类型 Codec 注册与解析入口。
 * 按 core 注册表创建类型，再交给对应 Codec 解码；注册表由应用启动期配置，不加载业务对象。
 * 可选类型解析与显式嵌套类型校验共用此入口，结构读取仅使用 Map。
 */
public class JetLinksDataTypeCodecs {

    private static final Map<String, DataTypeCodec<? extends DataType>> codecMap = new ConcurrentHashMap<>();

    static {
        register(new JetLinksArrayCodec());
        register(new JetLinksBooleanCodec());
        register(new JetLinksDateCodec());
        register(new JetLinksDoubleCodec());
        register(new JetLinksEnumCodec());
        register(new JetLinksFloatCodec());
        register(new JetLinksGeoPointCodec());
        register(new JetLinksIntCodec());
        register(new JetLinksLongCodec());
        register(new JetLinksObjectCodec());
        register(new JetLinksStringCodec());
        register(new JetLinksPasswordCodec());
        register(new JetLinksFileCodec());
        register(new JetLinksGeoShapeCodec());
        register(new JetLinksUserCodec());
        register(new JetLinksShortCodec());
        register(new JetLinksByteCodec());
    }

    private JetLinksDataTypeCodecs() {
    }

    public static void register(DataTypeCodec<? extends DataType> codec) {
        codecMap.put(codec.getTypeId(), codec);
    }

    @SuppressWarnings("all")
    public static Optional<DataTypeCodec<DataType>> getCodec(String typeId) {

        return Optional.ofNullable((DataTypeCodec) codecMap.get(typeId));
    }

    public static DataType decode(DataType type, Map<String, Object> config) {
        if (type == null) {
            return null;
        }
        return getCodec(type.getId())
                .map(codec -> codec.decode(type, config))
                .orElse(type);
    }

    /**
     * 解析可选对象类型定义；缺失或未注册的类型返回空，由所属模型决定缺省语义。
     *
     * @param config 类型对象定义，可为 null
     * @return 已由对应 Codec 解码的类型，或者空
     * @since 1.3.3
     */
    public static Optional<DataType> decode(Map<String, Object> config) {
        return Optional.ofNullable(config)
                       .map(definition -> definition.get("type"))
                       .map(String::valueOf)
                       .map(DataTypes::lookup)
                       .map(Supplier::get)
                       .map(type -> decode(type, config));
    }

    /**
     * 解析已显式声明的嵌套类型，兼容类型名简写和对象定义。
     * 可选字段的缺失由调用方处理，显式无效类型不能静默丢失。
     *
     * @param definition 类型名称或字符串键 Map
     * @return 已解码的已注册类型
     * @throws I18nSupportException 定义格式错误、没有类型名称或名称未注册
     * @since 1.3.3
     */
    public static DataType decodeNow(Object definition) {
        Map<String, Object> config = definition instanceof String
            ? Collections.singletonMap("type", definition)
            : MetadataDefinitionParser.parseObject(definition);
        return decode(config).orElseThrow(() -> new I18nSupportException.NoStackTrace(
            "error.invalid_metadata_data_type", Objects.toString(config.get("type"), "")));
    }

    public static Optional<Map<String, Object>> encode(DataType type) {
        if (type == null) {
            return Optional.empty();
        }
        return getCodec(type.getId())
                .map(codec -> codec.encode(type));
    }
}
