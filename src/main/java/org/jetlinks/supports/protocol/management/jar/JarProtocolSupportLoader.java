package org.jetlinks.supports.protocol.management.jar;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.core.trace.ProtocolTracer;
import org.jetlinks.core.utils.ClassUtils;
import org.jetlinks.supports.protocol.management.ProtocolSupportDefinition;
import org.jetlinks.supports.protocol.management.ProtocolSupportLoaderProvider;
import org.jetlinks.supports.protocol.validator.MethodDeniedClassVisitor;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class JarProtocolSupportLoader implements ProtocolSupportLoaderProvider {

    @Setter
    private ServiceContext serviceContext;

    private final Map<String, ProtocolClassLoader> protocolLoaders = new ConcurrentHashMap<>();

    private final Map<String, ProtocolSupportProvider> loaded = new ConcurrentHashMap<>();

    protected final MethodDeniedClassVisitor visitor = MethodDeniedClassVisitor.global();

    public JarProtocolSupportLoader() {

    }

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

    protected ServiceContext createServiceContext(ProtocolSupportDefinition definition) {
        return serviceContext;
    }

    @Override
    @SneakyThrows
    public Mono<? extends ProtocolSupport> load(ProtocolSupportDefinition definition) {
        String id = definition.getId();
        return Mono
            .defer(() -> {
                try {

                    Map<String, Object> config = definition.getConfiguration();
                    String location = Optional
                        .ofNullable(config.get("location"))
                        .map(String::valueOf)
                        .orElseThrow(() -> new IllegalArgumentException("location"));

                    URL url;

                    if (!location.contains("://")) {
                        url = new File(location).toURI().toURL();
                    } else {
                        url = new URL("jar:" + location + "!/");
                    }

                    ProtocolClassLoader loader;
                    URL fLocation = url;
                    {
                        ProtocolSupportProvider oldProvider = loaded.remove(id);
                        if (null != oldProvider) {
                            oldProvider.dispose();
                        }
                    }
                    loader = protocolLoaders.compute(id, (key, old) -> {
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
                    String provider = Optional
                        .ofNullable(config.get("provider"))
                        .map(String::valueOf)
                        .map(String::trim)
                        .orElse(null);
                    supportProvider = lookupProvider(provider, loader);
                    if (null == supportProvider) {
                        return Mono.error(new IllegalArgumentException("error.protocol_provider_not_found"));
                    }
                    ProtocolSupportProvider oldProvider = loaded.put(id, supportProvider);
                    try {
                        if (null != oldProvider) {
                            oldProvider.dispose();
                        }
                    } catch (Throwable e) {
                        log.error(e.getMessage(), e);
                    }
                    return supportProvider.create(createServiceContext(definition));
                } catch (Throwable e) {
                    return Mono.error(e);
                }
            })
            .subscribeOn(Schedulers.boundedElastic())
            .as(MonoTracer.create(ProtocolTracer.SpanName.install(id)));
    }

    @SneakyThrows
    protected ProtocolSupportProvider lookupProvider(String provider,
                                                     ProtocolClassLoader classLoader) {

        ClassUtils.Scanner<ProtocolClassLoader> loaderScanner = ClassUtils
            .createScanner(classLoader, "classpath:**/*.class", true);

        loaderScanner.walkClass((loader, name, clazz) -> visitor.validate(name, clazz));

        if (provider != null) {
            //直接从classLoad获取,防止冲突
            @SuppressWarnings("all")
            Class<ProtocolSupportProvider> providerType = (Class) classLoader.loadSelfClass(provider);
            return providerType.getDeclaredConstructor().newInstance();
        }

        return loaderScanner
            .findImplClass(ProtocolSupportProvider.class,
                           this::loadProvider)
            .orElse(null);
    }

    @SneakyThrows
    protected Class<?> loadProvider(ProtocolClassLoader loader, String className, InputStream classStream) {
        return loader.loadSelfClass(className);
    }
}
