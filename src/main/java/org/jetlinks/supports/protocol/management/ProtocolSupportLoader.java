package org.jetlinks.supports.protocol.management;

import org.jetlinks.core.ProtocolSupport;
import reactor.core.publisher.Mono;

public interface ProtocolSupportLoader {

    Mono<? extends ProtocolSupport> load(ProtocolSupportDefinition definition);
}
