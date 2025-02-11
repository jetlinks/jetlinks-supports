package org.jetlinks.supports.protocol.blocking;

import org.jetlinks.core.defaults.BlockingDeviceOperator;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.MessageEncodeContext;
import org.jetlinks.core.message.codec.ToDeviceMessageContext;
import org.jetlinks.core.monitor.Monitor;
import org.jetlinks.core.server.session.DeviceSession;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collection;

public class BlockingMessageEncodeContext extends BlockingMessageCodecContext<MessageEncodeContext> {


    public BlockingMessageEncodeContext(Monitor monitor,
                                        MessageEncodeContext context,
                                        Duration timeout,
                                        ContextView ctx) {
        super(monitor, context, timeout, ctx);
    }

    /**
     * 获取当前上下文的设备操作接口.
     *
     * @return 设备操作接口
     */
    @Nonnull
    @Override
    @SuppressWarnings("all")
    public BlockingDeviceOperator getDevice() {
        return super.getDevice();
    }

    /**
     * 获取指定设备ID的设备操作接口,如果设备不存在或者未激活将返回<code>null</code>.
     *
     * @param deviceId 设备ID
     * @return 设备操作接口
     */
    @Nullable
    @Override
    public BlockingDeviceOperator getDevice(String deviceId) {
        return super.getDevice(deviceId);
    }

    /**
     * 获取由平台下行给设备的的消息
     *
     * @return DeviceMessage
     * @see DeviceMessage
     * @see org.jetlinks.core.message.RepayableDeviceMessage
     * @see org.jetlinks.core.message.property.ReadPropertyMessage
     * @see org.jetlinks.core.message.property.WritePropertyMessage
     * @see org.jetlinks.core.message.function.FunctionInvokeMessage
     * @see org.jetlinks.core.message.firmware.UpgradeFirmwareMessage
     */
    public DeviceMessage getMessage() {
        return (DeviceMessage) context.getMessage();
    }

    /**
     * 获取当前设备的设备会话
     *
     * @return 设备会话
     */
    public DeviceSession getSession() {
        return context.unwrap(ToDeviceMessageContext.class).getSession();
    }

    /**
     * 获取指定设备的设备会话,只能获取到当前服务节点的设备会话.
     *
     * @param deviceId 设备ID
     * @return 设备会话
     */
    public DeviceSession getSession(String deviceId) {
        return await(context.unwrap(ToDeviceMessageContext.class).getSession(deviceId));
    }

    /**
     * 判断设备会话是否存活
     *
     * @param deviceId 设备ID
     * @return 是否存活
     */
    public boolean sessionIsAlive(String deviceId) {
        return await(
            context.unwrap(ToDeviceMessageContext.class).sessionIsAlive(deviceId)
        );
    }

    /**
     * 断开设备连接
     */
    public void disconnect() {
        await(
            context.unwrap(ToDeviceMessageContext.class).disconnect()
        );
    }

    /**
     * 发送数据到设备,此方法可能导致线程阻塞.
     * <p>
     * 注意: 避免在响应式上下文中执行此方法，否则可能导致性能问题。
     *
     * @param message 消息
     */
    public void sendToDeviceNow(EncodedMessage message) {
        await(context
                  .unwrap(ToDeviceMessageContext.class)
                  .sendToDevice(message));
    }


    /**
     * 异步发送数据到设备
     *
     * @param message 消息
     */
    public void sendToDeviceLater(EncodedMessage message) {
        async(context
                  .unwrap(ToDeviceMessageContext.class)
                  .sendToDevice(message));
    }

    /**
     * 响应式发送数据到设备
     *
     * @param message 消息
     */
    public Mono<Void> sendToDeviceReactive(EncodedMessage message) {
        return context
            .unwrap(ToDeviceMessageContext.class)
            .sendToDevice(message)
            .then();
    }

    /**
     * 异步发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     *
     * @param message 消息
     */
    public void sendToPlatformLater(Collection<? extends DeviceMessage> message) {
        async(
            context.reply(message)
        );
    }

    /**
     * 异步发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     *
     * @param message 消息
     */
    public void sendToPlatformLater(Publisher<? extends DeviceMessage> message) {
        async(
            context.reply(message)
        );
    }

    /**
     * 异步发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     *
     * @param message 消息
     */
    public void sendToPlatformLater(DeviceMessage message) {
        async(
            context.reply(message)
        );
    }

    /**
     * 响应式发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     *
     * @param message 消息
     */
    public Mono<Void> sendToPlatformReactive(DeviceMessage message) {
        return context.reply(message);
    }

    /**
     * 立即发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     * <p>
     * 注意: 避免在响应式上下文中执行此方法，否则可能导致性能问题。
     *
     * @param message 消息
     */
    public void sendToPlatformNow(Collection<? extends DeviceMessage> message) {
        await(context.reply(message));
    }

    /**
     * 立即发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     * <p>
     * 注意: 避免在响应式上下文中执行此方法，否则可能导致性能问题。
     *
     * @param message 消息
     */
    public void sendToPlatformNow(DeviceMessage message) {
        await(context.reply(message));
    }


}
