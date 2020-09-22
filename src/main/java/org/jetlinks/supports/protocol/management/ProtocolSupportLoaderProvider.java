package org.jetlinks.supports.protocol.management;

import org.jetlinks.core.ProtocolSupport;
import reactor.core.publisher.Mono;

/**
 * 协议加载服务商,用于根据配置加载协议支持
 *
 * @author zhouhao
 * @since 1.0.0
 */
public interface ProtocolSupportLoaderProvider {

    /**
     * @return 服务商标识
     */
    String getProvider();

    /**
     * 加载协议
     *
     * @param definition 协议配置
     * @return ProtocolSupport
     */
    Mono<? extends ProtocolSupport> load(ProtocolSupportDefinition definition);
}
