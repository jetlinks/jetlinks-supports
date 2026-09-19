package org.jetlinks.supports.cache;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.DataType;
import org.jetlinks.core.cache.FileQueue;
import org.jetlinks.supports.utils.MVStoreUtils;
import org.springframework.util.CompositeIterator;

import javax.annotation.Nonnull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
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
    private final AtomicInteger pollCursor = new AtomicInteger();
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
                builder -> MVStoreQueue.applyStoreOptions(builder, options, 64, 64 * 1024)
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
        return operationInStore(() -> pollOne(true));
    }

    @Override
    public T removeLast() {
        return operationInStore(() -> pollOne(false));
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
        return operationInStore(() -> {
            for (MVStoreQueue<T> shard : queues) {
                if (!shard.isEmpty()) {
                    return false;
                }
            }
            return true;
        });
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
        T temp = poll();
        if (temp == null) {
            throw new NoSuchElementException("No such element in file " + store.getFileStore().getFileName());
        }
        return temp;
    }

    @Override
    public T poll() {
        return operationInStore(() -> pollOne(true));
    }

    @Override
    public int poll(int size, Collection<? super T> container) {
        if (size <= 0) {
            return 0;
        }
        return operationInStore(() -> pollShardsFair(size, container, true));
    }

    @Override
    public int pollLast(int size, Collection<? super T> container) {
        if (size <= 0) {
            return 0;
        }
        return operationInStore(() -> pollShardsFair(size, container, false));
    }

    @Override
    public T element() {
        return operationInStore(() -> {
            T data = peekFromCursor();
            if (data == null) {
                throw new NoSuchElementException("No such element in file " + store.getFileStore().getFileName());
            }
            return data;
        });
    }

    @Override
    public T peek() {
        return operationInStore(this::peekFromCursor);
    }

    /**
     * Read path must not use {@link #QUEUE_HOLDER}: that FastThreadLocal is write affinity.
     * A sticky-first poll leaves other shards unread when the preferred shard already has a full batch.
     */
    private int nextPollStart(int shards) {
        if (shards <= 1) {
            return 0;
        }
        return Math.floorMod(pollCursor.getAndIncrement(), shards);
    }

    private int currentPollStart(int shards) {
        if (shards <= 1) {
            return 0;
        }
        return Math.floorMod(pollCursor.get(), shards);
    }

    private static int shardIndex(int start, int offset, int shards) {
        return shards <= 1 ? 0 : (start + offset) % shards;
    }

    private T pollOne(boolean fifo) {
        int shards = queues.size();
        int start = nextPollStart(shards);
        for (int k = 0; k < shards; k++) {
            MVStoreQueue<T> shard = queues.get(shardIndex(start, k, shards));
            T value = fifo ? shard.poll() : shard.removeLast();
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private int pollShardsFair(int size, Collection<? super T> container, boolean fifo) {
        int shards = queues.size();
        if (shards == 0) {
            return 0;
        }
        int start = nextPollStart(shards);
        int n = 0;
        for (int k = 0; k < shards; k++) {
            int remainingShards = shards - k;
            int remaining = size - n;
            int share = (remaining + remainingShards - 1) / remainingShards;
            n += pollShard(shardIndex(start, k, shards), share, container, fifo);
            if (n >= size) {
                return n;
            }
        }
        if (n < size) {
            for (int k = 0; k < shards; k++) {
                n += pollShard(shardIndex(start, k, shards), size - n, container, fifo);
                if (n >= size) {
                    return n;
                }
            }
        }
        return n;
    }

    private int pollShard(int index, int share, Collection<? super T> container, boolean fifo) {
        MVStoreQueue<T> shard = queues.get(index);
        return fifo ? shard.pollTo0(share, container) : shard.pollLastTo0(share, container);
    }

    private T peekFromCursor() {
        int shards = queues.size();
        int start = currentPollStart(shards);
        for (int k = 0; k < shards; k++) {
            MVStoreQueue<T> shard = queues.get(shardIndex(start, k, shards));
            if (shard.isEmpty()) {
                continue;
            }
            T value = shard.peek();
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
