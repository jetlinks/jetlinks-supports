package org.jetlinks.supports;

import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.ProtocolSupports;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultProtocolSupports implements ProtocolSupports {

    private Map<String, ProtocolSupport> supports = new ConcurrentHashMap<>();

    @Override
    public Mono<ProtocolSupport> getProtocol(String protocol) {
        ProtocolSupport support = supports.get(protocol);
        if (support == null) {
            return Mono.error(new UnsupportedOperationException("不支持的协议:" + protocol)) ;
        }
        return Mono.just(support);
    }

    public void register(ProtocolSupport support){
        supports.put(support.getId(),support);
    }
}
