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
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JarProtocolSupportLoader implements ProtocolSupportLoaderProvider {

    @Setter
    private ServiceContext serviceContext;

    private final Map<String, ProtocolClassLoader> protocolLoaders = new ConcurrentHashMap<>();

    private final Map<String, ProtocolSupportProvider> loaded = new ConcurrentHashMap<>();

    @Override
    public String getProvider() {
        return "jar";
    }

    protected ProtocolClassLoader createClassLoader(URL location) {
        return new ProtocolClassLoader(new URL[]{location}, this.getClass().getClassLoader());
    }

    protected void closeAll() {
        protocolLoaders.values().forEach(this::closeLoader);
        protocolLoaders.clear();
        loaded.clear();
    }

    @SneakyThrows
    protected void closeLoader(ProtocolClassLoader loader) {
        loader.close();
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
                        .map(String::valueOf)
                        .map(String::trim).orElse(null);
                URL url;

                if (!location.contains("://")) {
                    url = new File(location).toURI().toURL();
                } else {
                    url = new URL("jar:" + location + "!/");
                }

                ProtocolClassLoader loader;
                URL fLocation = url;
                loader = protocolLoaders.compute(definition.getId(), (key, old) -> {
                    if (null != old) {
                        try {
                            closeLoader(old);
                        } catch (Exception ignore) {
                        }
                    }
                    return createClassLoader(fLocation);
                });

                ProtocolSupportProvider supportProvider;
                log.debug("load protocol support from : {}", location);
                if (provider != null) {
                    supportProvider = (ProtocolSupportProvider) Class.forName(provider, true, loader).newInstance();
                } else {
                    supportProvider = ServiceLoader.load(ProtocolSupportProvider.class, loader).iterator().next();
                }
                ProtocolSupportProvider oldProvider = loaded.put(provider, supportProvider);
                try {
                    if (null != oldProvider) {
                        oldProvider.close();
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                return supportProvider.create(serviceContext);
            } catch (Exception e) {
                return Mono.error(e);
            }
        }).subscribeOn(Schedulers.elastic());
    }
}
