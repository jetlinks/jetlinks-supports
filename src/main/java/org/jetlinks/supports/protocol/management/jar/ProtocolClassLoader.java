package org.jetlinks.supports.protocol.management.jar;

import lombok.Getter;
import lombok.SneakyThrows;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

public class ProtocolClassLoader extends URLClassLoader {

    @Getter
    private final URL[] urls;

    public ProtocolClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.urls = urls;
    }

    @Override
    public void close() throws IOException {
        super.close();
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
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name) {
        if (StringUtils.isEmpty(name)) {
            return urls[0];
        }
        return super.findResource(name);
    }
}
