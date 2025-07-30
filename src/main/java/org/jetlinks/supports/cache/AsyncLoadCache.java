package org.jetlinks.supports.cache;

import lombok.AllArgsConstructor;
import org.jetlinks.core.utils.Reactors;
import org.slf4j.Logger;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class AsyncLoadCache<T> extends Mono<T> implements Disposable {

    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<AsyncLoadCache, Object>
        LOADED = AtomicReferenceFieldUpdater.newUpdater(AsyncLoadCache.class, Object.class, "loaded");

    @SuppressWarnings("all")
    private static final AtomicReferenceFieldUpdater<AsyncLoadCache, Sinks.One>
        AWAIT = AtomicReferenceFieldUpdater.newUpdater(AsyncLoadCache.class, Sinks.One.class, "await");

    private final Mono<T> loader;
    private volatile Sinks.One<T> await;

    private final Disposable.Swap loading = Disposables.swap();

    private volatile T loaded;

    public AsyncLoadCache(Mono<T> loader) {
        this.loader = loader;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    protected T loaded() {
        return (T) LOADED.get(this);
    }

    protected abstract Logger logger();

    @SuppressWarnings("all")
    protected void afterLoaded(T value) {

    }

    @Nonnull
    protected Throwable onLoadError(Throwable throwable) {
        return throwable;
    }

    protected Mono<T> requestOnDisposed() {
        return Mono.error(new IllegalStateException());
    }

    protected void dispose(T value) {
        if (value instanceof Disposable) {
            ((Disposable) value).dispose();
        }
    }

    protected abstract void doDispose();

    @Override
    public final void dispose() {
        synchronized (this) {
            if (loading.isDisposed()) {
                return;
            }
            loading.dispose();
        }
        @SuppressWarnings("unchecked")
        T loaded = (T) LOADED.getAndSet(this, null);
        if (loaded != null) {
            dispose(loaded);
        }
        doDispose();
    }

    public final boolean isDisposed() {
        return loading.isDisposed();
    }

    protected boolean isDisposed(T value) {
        return value instanceof Disposable && ((Disposable) value).isDisposed();
    }

    protected final Mono<T> tryLoad() {
        for (; ; ) {
            if (isDisposed()) {
                return requestOnDisposed();
            }
            // 已经加载成功
            @SuppressWarnings("unchecked")
            T loaded = (T) LOADED.get(this);
            if (loaded != null && !isDisposed(loaded)) {
                return Mono.just(loaded);
            }

            // 已经存在await
            @SuppressWarnings("unchecked")
            Sinks.One<T> await = AWAIT.get(this);
            if (await != null) {
                return await.asMono();
            }
            // 尝试加载
            Sinks.One<T> newAwait = Sinks.one();
            if (AWAIT.compareAndSet(this, null, newAwait)) {
                LoadingSubscriber<T> subscriber = new LoadingSubscriber<>(this, newAwait);
                loading.update(subscriber);
                try {
                    if (isDisposed()) {
                        continue;
                    }
                    loader.subscribe(subscriber);
                    return newAwait.asMono();
                } catch (Throwable e) {
                    AWAIT.compareAndSet(this, newAwait, null);
                    newAwait.emitError(e, Reactors.emitFailureHandler());
                    throw e;
                }
            }
        }
    }

    @Override
    public void subscribe(@Nonnull CoreSubscriber<? super T> actual) {
        tryLoad().subscribe(actual);
    }


    @AllArgsConstructor
    private static class LoadingSubscriber<T> extends BaseSubscriber<T> {
        private final AsyncLoadCache<T> parent;
        private final Sinks.One<T> awaitRef;

        @Override
        protected void hookOnNext(@Nonnull T value) {

            @SuppressWarnings("unchecked")
            T old = (T) LOADED.getAndSet(parent, value);
            if (old != null) {
                parent.dispose(old);
            }
            AWAIT.compareAndSet(parent, awaitRef, null);
            awaitRef.emitValue(value, Reactors.emitFailureHandler());

            parent.afterLoaded(value);
        }

        @Override
        protected void hookFinally(@Nonnull SignalType type) {
            if (parent.loading.get() == this) {
                parent.loading.replace(Disposables.disposed());
            }
            if (AWAIT.get(parent) == awaitRef) {
                awaitRef.emitEmpty(Reactors.emitFailureHandler());
            }
        }

        @Override
        protected void hookOnError(@Nonnull Throwable throwable) {
            if (Exceptions.isRetryExhausted(throwable)) {
                throwable = throwable.getCause();
            }
            throwable = parent.onLoadError(throwable);
            AWAIT.compareAndSet(parent, awaitRef, null);
            awaitRef.emitError(throwable, Reactors.emitFailureHandler());
        }
    }


}
