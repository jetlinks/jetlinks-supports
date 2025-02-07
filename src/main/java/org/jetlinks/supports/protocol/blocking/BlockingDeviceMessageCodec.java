package org.jetlinks.supports.protocol.blocking;

import org.jetlinks.core.command.CommandSupport;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.DeviceMetadata;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.ValidateResult;
import org.jetlinks.core.monitor.Monitor;
import org.jetlinks.core.monitor.logger.Logger;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.core.things.BlockingThingsDataManager;
import org.jetlinks.core.things.ThingsDataManager;
import org.jetlinks.core.utils.Reactors;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Map;

/**
 * 阻塞式的设备消息编解码器,可通过阻塞的方式操作对应的API.
 * <p>
 * 注意: 如果要阻塞获取响应式结果,请使用{@link BlockingDeviceMessageCodec#await(Mono)}
 * 或者{@link Reactors#await(Mono, Duration)}方法.
 * <p>
 * 请勿直接使用{@link Mono#block()}等阻塞方法.
 *
 * @author zhouhao
 * @since 1.2.3
 */
public abstract class BlockingDeviceMessageCodec implements DeviceMessageCodec {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    protected final ServiceContext context;
    protected final Transport transport;

    public BlockingDeviceMessageCodec(ServiceContext context,
                                      Transport transport) {
        this.context = context;
        this.transport = transport;
    }

    @Override
    public final Transport getSupportTransport() {
        return transport;
    }

    protected final Logger logger() {
        return context.getMonitor().logger();
    }

    protected final Logger logger(String deviceId) {
        return context.getMonitor(deviceId).logger();
    }

    /**
     * 处理设备上行消息,当设备向平台发送消息时,将调用此方法进行处理.
     *
     * @param context 解码上下文
     * @see BlockingMessageDecodeContext
     * @see BlockingMessageDecodeContext#getData()
     * @see BlockingMessageDecodeContext#sendToDeviceLater(EncodedMessage)
     * @see BlockingMessageDecodeContext#sendToPlatformLater(DeviceMessage)
     */
    protected abstract void upstream(BlockingMessageDecodeContext context);

    /**
     * 处理设备下行消息,当平台向设备发送消息时,将调用此方法进行处理.
     *
     * @param context 编码上下文
     */
    protected abstract void downstream(BlockingMessageEncodeContext context);

    /**
     * 获取阻塞超时时间,默认30秒,可通过重写此方法来自定义超时时间.
     *
     * @return 超时时间
     */
    protected Duration getBlockingTimeout() {
        return DEFAULT_TIMEOUT;
    }

    /**
     * 阻塞等待异步任务执行完成,并获取结果
     *
     * @param mono 异步任务
     * @param <T>  结果类型
     * @return 结果
     */
    protected <T> T await(Mono<T> mono) {
        return Reactors.await(mono, getBlockingTimeout());
    }

    /**
     * 获取命令服务,用于执行平台内部命令,如文件上传等功能. <a href="https://hanta.yuque.com/px7kg1/dev/ew1xvzmlgbzkc0hy#xhJ3I">查看文档</a>
     *
     * @param serviceId 服务ID
     * @return 命令服务
     */
    protected final CommandSupport getCommandService(String serviceId) {
        return context
            .getService(serviceId, CommandSupport.class)
            .orElseThrow(() -> new UnsupportedOperationException("unsupported commandService:" + serviceId));
    }

    /**
     * 获取设备物模型管理器,用于获取设备数据缓存信息.
     *
     * @return 设备物模型管理器
     */
    protected final BlockingThingsDataManager getDataManager() {
        return context
            .getService(ThingsDataManager.class)
            .map(manager -> new BlockingThingsDataManager(manager, getBlockingTimeout()))
            .orElseThrow(() -> new UnsupportedOperationException("unsupported service:ThingsDataManager"));
    }

    /**
     * 校验属性值,并放入到属性Map容器中,如果校验失败,将会记录日志.
     *
     * @param deviceId  设备ID
     * @param metadata  设备物模型
     * @param property  属性名称
     * @param value     属性值
     * @param container 属性Map容器
     */
    protected void validateAndPutProperty(String deviceId,
                                          DeviceMetadata metadata,
                                          String property,
                                          Object value,
                                          Map<String, Object> container) {
        //属性没有定义
        PropertyMetadata prop = metadata.getPropertyOrNull(property);
        if (prop == null) {
            logger(deviceId)
                .info("message.undefined_property_metadata", property, value);
            return;
        }
        DataType type = prop.getValueType();
        //类型校验失败
        ValidateResult validate = type.validate(value);
        if (!validate.isSuccess()) {
            logger(deviceId)
                .warn("message.invalid_property_value", property, value, validate.getErrorMsg());
            return;
        }
        container.put(property, validate.getValue());
    }

    @Nonnull
    @Override
    public final Publisher<? extends Message> decode(@Nonnull MessageDecodeContext context) {
        return Mono.deferContextual((ctx) -> {
            DeviceOperator device = context.getDevice();
            Monitor monitor = device == null ? this.context.getMonitor() : this.context.getMonitor(device.getDeviceId());
            //在非阻塞线程中,需要使用单独的调度器来执行.
            //在java21中,建议开启虚拟线程调度器. -Dreactor.schedulers.defaultBoundedElasticOnVirtualThreads=true
            if (isInNonBlocking()) {
                return Mono
                    .fromCallable(() -> {
                        BlockingMessageDecodeContext decodeContext =
                            new BlockingMessageDecodeContext(monitor, context, getBlockingTimeout(), ctx);

                        upstream(decodeContext);
                        return decodeContext;
                    })
                    .<Message>flatMap(BlockingMessageCodecContext::runAsync)
                    .subscribeOn(Schedulers.boundedElastic());
            } else {
                BlockingMessageDecodeContext decodeContext =
                    new BlockingMessageDecodeContext(monitor, context, getBlockingTimeout(), ctx);

                upstream(decodeContext);
                return decodeContext.runAsync();
            }
        });

    }

    @Nonnull
    @Override
    public final Publisher<? extends EncodedMessage> encode(@Nonnull MessageEncodeContext context) {
        return Mono.deferContextual((ctx) -> {
            DeviceOperator device = context.getDevice();
            Monitor monitor = device == null ? this.context.getMonitor() : this.context.getMonitor(device.getDeviceId());
            //在非阻塞线程中,需要使用单独的调度器来执行.
            //在java21中,建议开启虚拟线程调度器. -Dreactor.schedulers.defaultBoundedElasticOnVirtualThreads=true
            if (isInNonBlocking()) {
                return Mono
                    .fromCallable(() -> {
                        BlockingMessageEncodeContext encodeContext =
                            new BlockingMessageEncodeContext(monitor, context, getBlockingTimeout(), ctx);
                        downstream(encodeContext);
                        return encodeContext;
                    })
                    .<EncodedMessage>flatMap(BlockingMessageCodecContext::runAsync)
                    .subscribeOn(Schedulers.boundedElastic());
            } else {
                BlockingMessageEncodeContext encodeContext =
                    new BlockingMessageEncodeContext(monitor, context, getBlockingTimeout(), ctx);
                downstream(encodeContext);
                return encodeContext.runAsync();
            }
        });
    }

    protected boolean isInNonBlocking() {
        return Schedulers.isNonBlockingThread(Thread.currentThread());
    }
}
