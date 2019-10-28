package org.jetlinks.supports.protocol.management;

import org.jetlinks.core.ProtocolSupport;
import reactor.core.publisher.Mono;

public interface ProtocolSupportLoaderProvider  {

    String getProvider();

    Mono<? extends ProtocolSupport> load(ProtocolSupportDefinition definition);
}
