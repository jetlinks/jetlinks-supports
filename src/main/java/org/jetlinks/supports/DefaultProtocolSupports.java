package org.jetlinks.supports;

import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultProtocolSupports implements ProtocolSupports {

    private Map<String, ProtocolSupport> supports = new ConcurrentHashMap<>();

    @Override
    public ProtocolSupport getProtocol(String protocol) {
        ProtocolSupport support = supports.get(protocol);
        if (support == null) {
            throw new UnsupportedOperationException("不支持的协议:" + protocol);
        }
        return support;
    }

    public void register(ProtocolSupport support){
        supports.put(support.getId(),support);
    }
}
