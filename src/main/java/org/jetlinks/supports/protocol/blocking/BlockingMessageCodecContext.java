package org.jetlinks.supports.protocol.blocking;

import lombok.RequiredArgsConstructor;
import org.jetlinks.core.defaults.BlockingDeviceOperator;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.MessageCodecContext;
import org.jetlinks.core.monitor.Monitor;
import org.jetlinks.core.monitor.logger.Logger;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.core.trace.ReactiveSpanBuilder;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

@RequiredArgsConstructor
class BlockingMessageCodecContext<T extends MessageCodecContext> {

    final Monitor monitor;
    final T context;
    final Duration timeout;
    final ContextView ctx;

    private Queue<Mono<?>> async;

    /**
     * 获取当前上下文的设备操作接口,如果设备为首次连接,将会返回<code>null</code>.
     *
     * @return 设备操作接口
     */
    @Nullable
    public BlockingDeviceOperator getDevice() {
        return wrapBlocking(context.getDevice());
    }

    /**
     * 获取指定设备ID的设备操作接口,如果设备不存在或者未激活将返回<code>null</code>.
     *
     * @param deviceId 设备ID
     * @return 设备操作接口
     */
    @Nullable
    public BlockingDeviceOperator getDevice(String deviceId) {
        return wrapBlocking(await(context.getDevice(deviceId)));
    }

    /**
     * 响应式获取指定设备ID的设备操作接口
     *
     * @param deviceId 设备ID
     * @return 设备操作接口
     */
    @Nonnull
    public Mono<DeviceOperator> getDeviceAsync(String deviceId) {
        return context.getDevice(deviceId);
    }


    /**
     * 阻塞等待异步任务执行完成,并获取结果.
     *
     * <pre>{@code
     * // 伪代码
     * ConfigInfo conf = context.await(
     *     Mono.zip(
     *      redis.hmget("key", "field1", "field2"),
     *      device.getSelfConfig("token"),
     *      this::converterConfig
     *     )
     * );
     *
     * }</pre>
     *
     * @param async 异步任务
     * @param <R>   结果类型
     * @return 结果
     */
    public <R> R await(Mono<R> async) {
        return Reactors.await(async.contextWrite(ctx), timeout);
    }

    /**
     * 添加异步任务,在编解码最后执行这些任务.
     *
     * <pre>{@code
     *
     * // 伪代码
     *  context.async(
     *     // 发起http请求
     *     requestHttp(...)
     *     // 转换为设备消息
     *     .map(this::transformDeviceMessage)
     *     // 将设备消息发送给平台,注意: 这里只能使用响应式的方式发送.
     *     .flatMap(context::sendToPlatformReactive)
     *  )
     *
     * }</pre>
     * <p>
     * 注意：<ul>
     * <li>多个任务串行执行,一个任务报错会终止执行。</li>
     * </ul>
     *
     * @param async 异步任务
     */
    public synchronized void async(Mono<?> async) {
        if (this.async == null) {
            this.async = Queues.<Mono<?>>unboundedMultiproducer().get();
        }
        this.async.add(async);
    }

    <V> Mono<V> runAsync() {
        Queue<Mono<?>> async = this.async;
        if (async == null) {
            return Mono.empty();
        }
//       return Flux.<Mono<?>,Queue<Mono<?>>>generate(
//            () -> async,
//            (s, gen) -> {
//                Mono<?> task = s.poll();
//                if (task != null) {
//                    gen.next(task);
//                } else {
//                    gen.complete();
//                }
//                return s;
//            })
//            .concatMap(Function.identity(),0)
//            .then(Mono.empty());

        return new AsyncRunner<>(async);
    }

    @RequiredArgsConstructor
    private static class AsyncRunner<V> extends Mono<V> implements Subscription {

        private final Queue<Mono<?>> async;
        private volatile CoreSubscriber<? super V> actual;

        @SuppressWarnings("all")
        private static final AtomicReferenceFieldUpdater<AsyncRunner, BaseSubscriber>
            PENDING = AtomicReferenceFieldUpdater.newUpdater(
            AsyncRunner.class, BaseSubscriber.class, "pending"
        );
        private volatile BaseSubscriber<Object> pending;

        @Override
        public void subscribe(@Nonnull CoreSubscriber<? super V> actual) {
            synchronized (this) {
                if (this.actual != null) {
                    actual.onError(Exceptions.duplicateOnSubscribeException());
                    return;
                }
                this.actual = actual;
            }
            this.actual.onSubscribe(this);
        }


        void darin() {
            Mono<?> task;
            synchronized (async) {
                task = async.poll();
            }
            if (task == null) {
                actual.onComplete();
                return;
            }
            BaseSubscriber<Object> subscriber =
                new BaseSubscriber<Object>() {
                    @Override
                    @Nonnull
                    public Context currentContext() {
                        return actual.currentContext();
                    }

                    @Override
                    protected void hookOnError(@Nonnull Throwable throwable) {
                        actual.onError(throwable);
                    }

                    @Override
                    protected void hookFinally(@Nonnull SignalType type) {
                        if (type == SignalType.ON_COMPLETE) {
                            darin();
                        }
                    }
                };

            PENDING.set(this, subscriber);
            if (Schedulers.isNonBlockingThread(Thread.currentThread())) {
                task.subscribe(subscriber);
            } else {
                //在阻塞线程中,使用单独的调度器来执行.
                Schedulers
                    .parallel()
                    .schedule(() -> task.subscribe(subscriber));
            }

        }

        @Override
        public void request(long n) {
            darin();
        }

        @Override
        public void cancel() {
            synchronized (this.async) {
                this.async.clear();
            }
            @SuppressWarnings("all")
            BaseSubscriber<Object> pending = PENDING.getAndSet(this, null);
            if (pending != null) {
                pending.cancel();
            }
        }
    }

    public Monitor monitor() {
        return monitor;
    }

    public Logger logger() {
        return monitor.logger();
    }

    private BlockingDeviceOperator wrapBlocking(DeviceOperator device) {
        if (device == null) {
            return null;
        }
        return new BlockingDeviceOperator(device, timeout, ctx);
    }


}
