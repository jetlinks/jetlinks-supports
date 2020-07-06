package org.jetlinks.supports.protocol.management.jar;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

public class ProtocolClassLoader extends URLClassLoader {

    public ProtocolClassLoader(String location, ClassLoader parent) throws Exception {
        this(new URL[]{new URL(location)}, parent);
    }

    public ProtocolClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

}
