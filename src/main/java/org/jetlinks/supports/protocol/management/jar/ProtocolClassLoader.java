package org.jetlinks.supports.protocol.management.jar;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

public class ProtocolClassLoader extends URLClassLoader {

    public ProtocolClassLoader(String location, ClassLoader parent) throws Exception {
        super(new URL[]{new URL(location)}, parent);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

}
