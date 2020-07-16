package org.jetlinks.supports.protocol.management;

import org.jetlinks.core.ProtocolSupport;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultiProtocolSupportLoader implements ProtocolSupportLoader {

    private final Map<String, ProtocolSupportLoaderProvider> providers = new ConcurrentHashMap<>();

    @Override
    public Mono<? extends ProtocolSupport> load(ProtocolSupportDefinition definition) {
        return Mono.justOrEmpty(providers.get(definition.getProvider()))
                .switchIfEmpty(Mono.error(() -> new UnsupportedOperationException("unsupported provider:" + definition.getProvider())))
                .flatMap(provider -> provider.load(definition));
    }

    public void register(ProtocolSupportLoaderProvider provider) {
        providers.put(provider.getProvider(), provider);
    }
}
