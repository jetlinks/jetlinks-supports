package org.jetlinks.supports.cache;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.OffHeapStore;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.LongDataType;
import org.jetlinks.core.cache.FileQueue;
import org.jetlinks.core.codec.Codec;
import org.jetlinks.core.config.ConfigKey;
import org.jetlinks.core.utils.CompositeCollection;
import org.jetlinks.supports.utils.MVStoreUtils;
import org.springframework.util.Assert;
import org.springframework.util.CompositeIterator;
import reactor.core.scheduler.Schedulers;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于 <a href="http://www.h2database.com/html/mvstore.html">h2database mvstore</a>实现的本地队列,可使用此队列进行数据本地持久化
 *
 * @param <T> Type
 */
@Slf4j
class ConcurrencyMVStoreQueue<T> implements FileQueue<T> {

    private MVStore store;

    private final AtomicInteger inc = new AtomicInteger();
    private final FastThreadLocal<Integer> QUEUE_HOLDER;

    private final List<MVStoreQueue<T>> queues;
    private final Path filePath;
    private final String name;
    private final Map<String, Object> options;
    private final int concurrency;
    private final AtomicBoolean loading = new AtomicBoolean();
    private final ReadWriteLock loadLock = new ReentrantReadWriteLock();
    private boolean closed;

    @SneakyThrows
    ConcurrencyMVStoreQueue(Path filePath,
                            String name,
                            Map<String, Object> options,
                            int concurrency) {
        Files.createDirectories(filePath);
        this.filePath = filePath;
        this.name = name;
        this.options = options;
        this.concurrency = concurrency;
        this.queues = new ArrayList<>(concurrency);
        this.QUEUE_HOLDER = new FastThreadLocal<Integer>() {
            @Override
            protected Integer initialValue() {
                return inc.accumulateAndGet(1, (a, b) -> a + 1 >= queues.size() ? 0 : a + 1);
            }
        };
        init();

    }

    private void init() {
        loadLock.writeLock().lock();
        try {
            if (!loading.compareAndSet(false, true) || closed) {
                return;
            }
            if (store != null) {
                store.close(1000);
            }
            store = MVStoreUtils.open(
                filePath.resolve(name).toFile(),
                name,
                builder -> builder
                    .cacheSize(64)
                    .autoCommitBufferSize(64 * 1024)
                    .backgroundExceptionHandler(((t, e) -> log.warn("{} UncaughtException", name, e))),
                store -> {
                    queues.clear();
                    Object type = options.get("valueType");
                    MVMap.Builder<Long, T> mapBuilder = new MVMap.Builder<>();
                    if (type instanceof DataType) {
                        mapBuilder.valueType(((DataType<T>) type));
                    }
                    Set<String> queueNames = new HashSet<>();
                    for (int i = 0; i < concurrency; i++) {
                        String mapName = i == 0 ? "queue" : "queue_" + i;
                        queueNames.add(mapName);
                        queues.add(new MVStoreQueue<>(MVStoreUtils.openMap(store, mapName, mapBuilder)));
                    }
                    for (String mapName : store.getMapNames()) {
                        if (mapName.startsWith("queue")) {
                            //缩小了并行度?
                            if (queueNames.add(mapName)) {
                                queues.add(new MVStoreQueue<>(MVStoreUtils.openMap(store, mapName, mapBuilder)));
                            }
                        }
                    }
                    return store;
                }
            );
        } finally {
            loading.set(false);
            loadLock.writeLock().unlock();
        }

    }

    @SneakyThrows
    private <X> X operationInStore(Callable<X> call) {
        int retry = 0;
        Throwable error;
        do {
            loadLock.readLock().lock();
            try {
                return call.call();
            } catch (Throwable e) {
                error = e;
                log.warn("operation mvstore failed!", e);
            } finally {
                loadLock.readLock().unlock();
            }
            init();
        } while (retry++ == 0);
        throw error;
    }

    @Override
    public void close() {
        if (closed || store.isClosed()) {
            return;
        }
        closed = true;
        if (size() >= 100_0000) {
            store.close(20_000);
        } else {
            store.close(-1);
        }
        QUEUE_HOLDER.remove();
    }

    @Override
    public void flush() {
        if (store.isClosed()) {
            return;
        }
        // store.commit();
        store.compactFile((int) Duration.ofSeconds(30).toMillis());
    }

    @Override
    public T removeFirst() {
        return operationInStore(() -> {
            T temp;
            for (MVStoreQueue<T> queue : queues) {
                temp = queue.removeFirst();
                if (temp != null) {
                    return temp;
                }
            }
            return null;
        });
    }

    @Override
    public T removeLast() {
        return operationInStore(() -> {
            T temp;
            for (MVStoreQueue<T> queue : queues) {
                temp = queue.removeLast();
                if (temp != null) {
                    return temp;
                }
            }
            return null;
        });
    }

    @Override
    public int size() {
        return operationInStore(() -> {
            int size = 0;
            for (MVStoreQueue<T> queue : queues) {
                size += queue.size();
            }
            return size;
        });
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        CompositeIterator<T> iterator = new CompositeIterator<>();
        for (MVStoreQueue<T> queue : queues) {
            iterator.add(queue.iterator());
        }
        return iterator;
    }

    @Override
    @Nonnull
    public Object[] toArray() {
        throw new UnsupportedOperationException("unsupported operation");
    }

    @Override
    @Nonnull
    public <T1> T1[] toArray(@Nonnull T1[] a) {
        throw new UnsupportedOperationException("unsupported operation");
    }


    @Override
    public boolean add(T t) {
        return operationInStore(() -> queues
            .get(QUEUE_HOLDER.get())
            .add(t));
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return operationInStore(() -> queues
            .get(QUEUE_HOLDER.get())
            .addAll(c));
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        operationInStore(() -> {
            for (MVStoreQueue<T> queue : queues) {
                queue.clear();
            }
            return null;
        });
    }

    @Override
    public boolean offer(T t) {
        return operationInStore(() -> queues
            .get(QUEUE_HOLDER.get())
            .offer(t));
    }

    @Override
    public T remove() {
        for (MVStoreQueue<T> queue : queues) {
            T temp = queue.poll();
            if (temp != null) {
                return temp;
            }
        }
        throw new NoSuchElementException("No such element in file " + store.getFileStore().getFileName());
    }

    @Override
    public T poll() {
        return operationInStore(() -> {
            T poll = queues
                .get(QUEUE_HOLDER.get())
                .poll();
            if (poll == null) {
                for (MVStoreQueue<T> queue : queues) {
                    poll = queue.poll();
                    if (poll != null) {
                        return poll;
                    }
                }
            }
            return poll;
        });
    }

    @Override
    public T element() {
        return operationInStore(() -> {
            T poll = queues
                .get(QUEUE_HOLDER.get())
                .element();
            if (poll == null) {
                for (MVStoreQueue<T> queue : queues) {
                    poll = queue.element();
                    if (poll != null) {
                        return poll;
                    }
                }
            }
            return poll;
        });
    }

    @Override
    public T peek() {
        return operationInStore(() -> {
            T poll = queues
                .get(QUEUE_HOLDER.get())
                .peek();
            if (poll == null) {
                for (MVStoreQueue<T> queue : queues) {
                    poll = queue.peek();
                    if (poll != null) {
                        return poll;
                    }
                }
            }
            return poll;
        });
    }
}
