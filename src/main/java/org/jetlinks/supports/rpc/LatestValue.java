package org.jetlinks.supports.rpc;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import java.util.LinkedHashSet;
import java.util.Set;

/** 完整视图的有界观察：原子初始值、每观察者 latest slot，重入更新也保持版本顺序。 */
public final class LatestValue<T> {
    private volatile T value;
    private final Set<FluxSink<T>> observers = new LinkedHashSet<>();
    private boolean emitting, closed;
    private Throwable failure;

    public LatestValue(T initial) { value = initial; }
    public T current() { return value; }

    public Flux<T> watch() {
        return Flux.create(sink -> {
            synchronized (this) {
                if (failure != null) { sink.error(failure); return; }
                if (!closed) observers.add(sink);
                sink.onDispose(() -> remove(sink));
                sink.next(value);
                if (closed) sink.complete();
            }
        }, FluxSink.OverflowStrategy.LATEST);
    }

    private synchronized void remove(FluxSink<T> observer) { observers.remove(observer); }

    public synchronized void update(T next) {
        if (closed) return;
        value = next;
        if (emitting) return;
        emitting = true;
        try {
            T emitted;
            do {
                emitted = value;
                for (FluxSink<T> observer : Set.copyOf(observers)) observer.next(emitted);
            } while (emitted != value);
        } finally { emitting = false; }
    }

    public synchronized void close() {
        finish(null);
    }
    public synchronized void fail(Throwable error) { finish(error); }
    private void finish(Throwable error) {
        if (closed) return;
        closed = true;
        failure = error;
        for (FluxSink<T> observer : Set.copyOf(observers)) {
            if (error == null) observer.complete(); else observer.error(error);
        }
        observers.clear();
    }
}
