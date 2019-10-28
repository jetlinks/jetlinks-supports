package org.jetlinks.supports.protocol.management.jar;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.supports.protocol.management.ProtocolSupportDefinition;
import org.jetlinks.supports.protocol.management.ProtocolSupportLoaderProvider;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JarProtocolSupportLoader implements ProtocolSupportLoaderProvider {

    @Setter
    private ServiceContext serviceContext;

    private Map<String, ProtocolClassLoader> protocolLoaders = new ConcurrentHashMap<>();

    @Override
    public String getProvider() {
        return "jar";
    }

    @Override
    @SneakyThrows

    public Mono<? extends ProtocolSupport> load(ProtocolSupportDefinition definition) {
        return Mono.defer(() -> {
            try {
                Map<String, Object> config = definition.getConfiguration();
                String location = Optional.ofNullable(config.get("location"))
                        .map(String::valueOf).orElseThrow(() -> new IllegalArgumentException("location"));

                String provider = Optional.ofNullable(config.get("provider"))
                        .map(String::valueOf).orElse(null);

                if (!location.contains("://")) {
                    location = "file://" + location;
                }
                location = "jar:" + location + "!/";
                log.debug("load protocol support from : {}", location);
                ProtocolClassLoader loader;
                ProtocolClassLoader old = protocolLoaders.put(definition.getId(), loader = new ProtocolClassLoader(location, this.getClass().getClassLoader()));
                if (null != old) {
                    old.close();
                }
                ProtocolSupportProvider supportProvider;

                if (provider != null) {
                    supportProvider = (ProtocolSupportProvider) loader.loadClass(provider).newInstance();
                } else {
                    supportProvider = ServiceLoader.load(ProtocolSupportProvider.class, loader).iterator().next();
                }
                return supportProvider.create(serviceContext);
            } catch (Exception e) {
                return Mono.error(e);
            }
        });
    }
}
