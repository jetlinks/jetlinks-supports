package org.jetlinks.supports.protocol.management.jar;

import lombok.Getter;

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

}
