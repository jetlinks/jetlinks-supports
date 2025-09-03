package org.jetlinks.supports.utils;

import com.alibaba.fastjson.JSONObject;
import org.jetlinks.core.metadata.FunctionMetadata;
import org.jetlinks.supports.official.JetLinksDeviceFunctionMetadata;

import java.util.Map;

public class MetadataConverter {

    @SuppressWarnings("all")
    public static FunctionMetadata convertToFunctionMetadata(Object data) {
        if (data instanceof FunctionMetadata) {
            return ((FunctionMetadata) data);
        }
        if (data instanceof Map) {
            return new JetLinksDeviceFunctionMetadata(new JSONObject((Map<String, Object>) data));
        }
        return null;
    }
}
