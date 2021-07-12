package org.jetlinks.supports.cache;

import org.jetlinks.core.cache.FileQueue;
import org.jetlinks.core.cache.FileQueueBuilderFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class MVStoreQueueBuilderFactory implements FileQueueBuilderFactory {

    static Path defaultPath = Paths.get(System.getProperty("queue.file.path.default","./.queue"));

    @Override
    public <T> FileQueue.Builder<T> create() {
        FileQueue.Builder<T> builder = new MVStoreQueue.Builder<>();

        return builder.path(defaultPath);
    }
}
