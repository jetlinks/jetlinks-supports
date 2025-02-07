package org.jetlinks.supports.protocol.blocking;

import lombok.RequiredArgsConstructor;
import org.jetlinks.core.defaults.BlockingDeviceOperator;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.MessageCodecContext;
import org.jetlinks.core.monitor.Monitor;
import org.jetlinks.core.utils.Reactors;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import javax.annotation.Nullable;
import java.time.Duration;

@RequiredArgsConstructor
class BlockingMessageCodecContext<T extends MessageCodecContext> {

    final Monitor monitor;
    final T context;
    final Duration timeout;
    final ContextView ctx;

    private Mono<?> async;

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
     * 阻塞等待异步任务执行完成,并获取结果
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
     * <p>
     * 注意：多个任务串行执行,一个任务报错会终止执行。
     *
     * @param async 异步任务
     */
    public synchronized void async(Mono<?> async) {
        if (this.async == null) {
            this.async = async;
        } else {
            this.async = this.async.then(async);
        }
    }

    <V> Mono<V> runAsync() {
        return async == null ? Mono.empty() : async.then(Mono.empty());
    }

    private BlockingDeviceOperator wrapBlocking(DeviceOperator device) {
        if (device == null) {
            return null;
        }
        return new BlockingDeviceOperator(device, timeout, ctx);
    }


}
