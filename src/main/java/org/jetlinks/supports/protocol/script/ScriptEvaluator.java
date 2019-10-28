package org.jetlinks.supports.protocol.script;

import java.util.Map;

public interface ScriptEvaluator {
    Object evaluate(String lang, String script, Map<String, Object> context) throws Exception;

}
