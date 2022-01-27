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

/**
 * 动态加载Jar包来实现协议支持.
 * jar包不支持使用第三方依赖.
 * 如果需要依赖第三方包，可以使用maven-shade-plugin方式，将依赖class和协议打包在一起。
 * <p>
 * 配置格式:<pre>{@code
 * {
 *    "location":"jar文件地址",
 *    "provider":"ProtocolSupportProvider实现类全名"
 * }
 * }</pre>
 *
 * @author zhouhao
 * @see ProtocolSupportProvider
 * @since 1.0
 */
@Slf4j
public class JarProtocolSupportLoader implements ProtocolSupportLoaderProvider {

    @Setter
    private ServiceContext serviceContext;

    //ClassLoader缓存
    private final Map<String, ProtocolClassLoader> protocolLoaders = new ConcurrentHashMap<>();

    //已加载的协议提供商
    private final Map<String, ProtocolSupportProvider> loaded = new ConcurrentHashMap<>();

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
                //文件地址
                String location = Optional
                        .ofNullable(config.get("location"))
                        .map(String::valueOf)
                        .orElseThrow(() -> new IllegalArgumentException("location"));
                //ProtocolSupportProvider类全名
                String provider = Optional.ofNullable(config.get("provider"))
                                          .map(String::valueOf)
                                          .map(String::trim).orElse(null);
                URL url;

                if (!location.contains("://")) {
                    //本地文件
                    url = new File(location).toURI().toURL();
                } else {
                    //url
                    url = new URL("jar:" + location + "!/");
                }

                ProtocolClassLoader loader;
                URL fLocation = url;
                {
                    //释放旧的provider
                    ProtocolSupportProvider oldProvider = loaded.remove(provider);
                    if (null != oldProvider) {
                        oldProvider.dispose();
                    }
                }
                loader = protocolLoaders.compute(definition.getId(), (key, old) -> {
                    if (null != old) {
                        try {
                            //释放旧的ClassLoader
                            closeLoader(old);
                        } catch (Exception ignore) {
                        }
                    }
                    return createClassLoader(fLocation);
                });

                ProtocolSupportProvider supportProvider;
                log.debug("load protocol support from : {}", location);
                if (provider != null) {
                    //直接从classLoad获取,防止冲突
                    @SuppressWarnings("all")
                    Class<ProtocolSupportProvider> providerType = (Class) loader.loadSelfClass(provider);
                    supportProvider = providerType.getDeclaredConstructor().newInstance();
                } else {
                    //没有指定provider，则通过SPI来获取
                    supportProvider = ServiceLoader.load(ProtocolSupportProvider.class, loader).iterator().next();
                }

                ProtocolSupportProvider oldProvider = loaded.put(provider, supportProvider);
                try {
                    //如果存在旧的则释放掉
                    if (null != oldProvider) {
                        oldProvider.dispose();
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                //创建协议支持
                return supportProvider.create(serviceContext);
            } catch (Throwable e) {
                return Mono.error(e);
            }
        }).subscribeOn(Schedulers.elastic());
    }

    /**
     * 根据URL创建ClassLoader
     *
     * @param location location
     * @return ProtocolClassLoader
     */
    protected ProtocolClassLoader createClassLoader(URL location) {
        return new ProtocolClassLoader(new URL[]{location}, this.getClass().getClassLoader());
    }

    /**
     * 释放全部信息
     */
    protected void closeAll() {
        protocolLoaders.values().forEach(this::closeLoader);
        protocolLoaders.clear();
        loaded.values().forEach(ProtocolSupportProvider::dispose);
        loaded.clear();
    }

    /**
     * 关闭(释放)classLoader
     *
     * @param loader classLoader
     */
    @SneakyThrows
    protected void closeLoader(ProtocolClassLoader loader) {
        loader.close();
    }
}
