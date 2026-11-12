package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.ArrayType;
import org.jetlinks.supports.official.JetLinksDataTypeCodecs;

import java.util.Map;

import static java.util.Optional.ofNullable;

/**
 * 默认数组类型 Codec，保留可选元素类型及类型名简写。
 * 显式元素定义通过公共类型解析入口校验，不加载业务模型或忽略未注册类型。
 *
 * @see JetLinksDataTypeCodecs#decodeNow(Object)
 */
@Getter
@Setter
public class JetLinksArrayCodec extends AbstractDataTypeCodec<ArrayType> {

    @Override
    public String getTypeId() {
        return ArrayType.ID;
    }

    @Override
    public ArrayType decode(ArrayType type, Map<String, Object> config) {
        super.decode(type, config);
        ofNullable(config.get("elementType"))
            .map(JetLinksDataTypeCodecs::decodeNow)
            .ifPresent(type::setElementType);

        return type;
    }

    @Override
    protected void doEncode(Map<String, Object> encoded, ArrayType type) {
        super.doEncode(encoded, type);
        if (type != null && type.getElementType()!=null) {
            JetLinksDataTypeCodecs
                .getCodec(type.getElementType().getId())
                .ifPresent(codec -> encoded.put("elementType", codec.encode(type.getElementType())));
        }

    }
}
