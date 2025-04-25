package org.jetlinks.supports.protocol.management.jar;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.monitor.Monitor;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

@Slf4j
public class ProtocolClassLoader extends URLClassLoader {

    @Getter
    private final URL[] urls;

    private final Monitor monitor;

    public ProtocolClassLoader(URL[] urls, ClassLoader parent) {
        this(null, urls, parent);
    }

    public ProtocolClassLoader(Monitor monitor, URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.urls = urls;
        this.monitor = monitor;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    protected Class<?> injectMonitor(Class<?> clazz) {
        if (this.monitor == null) {
            return clazz;
        }

        return clazz;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            Class<?> clazz = loadSelfClass(name);
            if (null != clazz) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (Throwable ignore) {

        }
        return super.loadClass(name, resolve);
    }

    @SneakyThrows
    public synchronized Class<?> loadSelfClass(String name) {
        Class<?> clazz = super.findLoadedClass(name);
        if (clazz == null) {
            clazz = super.findClass(name);
            // classloader自己定义的类，则注入监控.
            clazz = injectMonitor(clazz);

            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name) {
        if (StringUtils.hasText(name)) {
            return super.findResource(name);
        }
        return urls[0];
    }
}
