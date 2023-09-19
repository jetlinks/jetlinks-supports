package org.jetlinks.supports.cache;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.DataType;
import org.jetlinks.core.cache.FileQueue;
import org.jetlinks.core.codec.Codec;
import org.jetlinks.core.config.ConfigKey;
import org.jetlinks.supports.utils.MVStoreUtils;
import org.springframework.util.Assert;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于 <a href="http://www.h2database.com/html/mvstore.html">h2database mvstore</a>实现的本地队列,可使用此队列进行数据本地持久化
 *
 * @param <T> Type
 */
@Slf4j
class MVStoreQueue<T> implements FileQueue<T> {

    @SuppressWarnings("all")
    private final static AtomicLongFieldUpdater<MVStoreQueue> INDEX =
            AtomicLongFieldUpdater.newUpdater(MVStoreQueue.class, "index");

    private MVStore store;
    private MVMap<Long, T> mvMap;

    private volatile long index = 0;

    private final String name;

    private final Path storageFile;

    private final Map<String, Object> options;

    private final ReentrantLock pollLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock();

    @SneakyThrows
    MVStoreQueue(Path filePath,
                 String name,
                 Map<String, Object> options) {
        Files.createDirectories(filePath);
        this.name = name;
        this.storageFile = filePath.resolve(name);
        this.options = options;
        open();
    }

    protected void open() {
        try {
            if (store != null && !store.isClosed()) {
                store.close();
            }
        } catch (Throwable ignore) {

        }

        store = MVStoreUtils.open(
                storageFile.toFile(),
                name,
                builder -> builder
                        .cacheSize(16)
                        .autoCommitBufferSize(32 * 1024)
                        .compress());
        Object type = options.get("valueType");

        MVMap.Builder<Long, T> mapBuilder = new MVMap.Builder<>();
        if (type instanceof DataType) {
            mapBuilder.valueType(((DataType<T>) type));
        }

        mvMap = store.openMap("queue", mapBuilder);
        if (!mvMap.isEmpty()) {
            INDEX.set(this, mvMap.lastKey());
        }

    }


    @Override
    public void flush() {
        if (store.isClosed()) {
            return;
        }
        store.commit();
        store.compactFile((int) Duration.ofSeconds(30).toMillis());
    }

    @Override
    public T removeFirst() {
        checkClose();
        pollLock.lock();
        try {
            Long key = mvMap.firstKey();
            return key == null ? null : mvMap.remove(key);
        } finally {
            pollLock.unlock();
        }
    }

    @Override
    public T removeLast() {
        checkClose();
        pollLock.lock();
        try {
            Long key = mvMap.lastKey();
            return key == null ? null : mvMap.remove(key);
        } finally {
            pollLock.unlock();
        }
    }

    @Override
    public synchronized void close() {
        if (store.isClosed()) {
            return;
        }
        store.compactFile((int)Duration.ofSeconds(30).toMillis());
        store.sync();
        store.close();
    }

    private void checkClose() {
        if (store.isClosed()) {
            throw new IllegalStateException("file queue " + name + " is closed");
        }
    }

    @Override
    public int size() {
        checkClose();
        return mvMap.size();
    }

    @Override
    public boolean isEmpty() {
        checkClose();
        return mvMap.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        checkClose();
        return mvMap.containsValue(o);
    }

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        checkClose();
        Cursor<Long, T> cursor = mvMap.cursor(null, null, false);

        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return cursor.hasNext();
            }

            @Override
            public T next() {
                return cursor.getValue();
            }

            @Override
            public void remove() {
                mvMap.remove(cursor.getKey());
            }
        };
    }

    @Override
    @Nonnull
    public Object[] toArray() {
        return toArray(new Object[0]);
    }

    @Override
    @Nonnull
    public <T1> T1[] toArray(@Nonnull T1[] a) {
        checkClose();
        return stream().toArray((i) -> a);
    }

    @Override
    public boolean add(T t) {
        checkClose();
        if (null == t) {
            return false;
        }
        //lock, 多线程下,mvMap的锁可能导致性能问题
        writeLock.lock();
        try {
            doAdd(t);
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    private void doAdd(T value) {
        T val = value;
        do {
            val = mvMap.putIfAbsent(INDEX.incrementAndGet(this), val);
        } while (val != null);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("remove unsupported");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        checkClose();
        return mvMap.values().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        checkClose();
        writeLock.lock();
        try {
            for (T t : c) {
                doAdd(t);
            }
        } finally {
            writeLock.unlock();
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("removeAll unsupported");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll unsupported");
    }

    @Override
    public void clear() {
        if (mvMap.isClosed()) {
            return;
        }
        mvMap.clear();
        INDEX.set(this, 0);
    }

    @Override
    public boolean offer(T t) {
        checkClose();
        return add(t);
    }

    @Override
    public T remove() {
        checkClose();
        T data = poll();
        if (data == null) {
            throw new NoSuchElementException("No such element in file " + storageFile);
        }
        return data;
    }

    @Override
    public T poll() {
        if (mvMap.isClosed()) {
            return null;
        }
        T removed;
        try {
            pollLock.lock();
            Long key = mvMap.firstKey();
            removed = key == null ? null : mvMap.remove(key);
        } finally {
            pollLock.unlock();
        }
        return removed;

    }

    @Override
    public T element() {
        if (mvMap.isClosed()) {
            return null;
        }
        T data = peek();
        if (data == null) {
            throw new NoSuchElementException("No such element in file " + storageFile);
        }
        return data;
    }

    @Override
    public T peek() {
        checkClose();
        return mvMap.get(mvMap.firstKey());
    }


    static class Builder<T> implements FileQueue.Builder<T> {
        private String name;
        private Codec<T> codec;
        private Path path;

        private Map<String, Object> options = new HashMap<>();

        @Override
        public FileQueue.Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public FileQueue.Builder<T> codec(Codec<T> codec) {
            this.codec = codec;
            return this;
        }

        @Override
        public FileQueue.Builder<T> path(Path path) {
            this.path = path;
            return this;
        }

        @Override
        public FileQueue.Builder<T> options(Map<String, Object> options) {
            this.options.putAll(options);
            return this;
        }

        @Override
        public FileQueue.Builder<T> option(String key, Object value) {
            this.options.put(key, value);
            return this;
        }

        @Override
        public <V> FileQueue.Builder<T> option(ConfigKey<V> key, V value) {
            this.options.put(key.getName(), value);
            return this;
        }

        @Override
        public FileQueue<T> build() {
            Assert.hasText(name, "name must not be empty");
            Assert.notNull(path, "path must not be null");
            Assert.notNull(path, "codec must not be null");
            return new MVStoreQueue<>(path, name, options);
        }
    }
}
