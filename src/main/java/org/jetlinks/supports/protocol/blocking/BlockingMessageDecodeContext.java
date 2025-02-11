package org.jetlinks.supports.protocol.blocking;

import org.jetlinks.core.defaults.BlockingDeviceOperator;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.message.codec.ToDeviceMessageContext;
import org.jetlinks.core.monitor.Monitor;
import org.jetlinks.core.server.ClientConnection;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * 阻塞式的设备消息解码上下文,通过阻塞的方式操作对应的API.
 *
 * <pre>{@code
 *
 *  public void decode(BlockingMessageDecodeContext ctx){
 *      ByteBuf data = ctx.getData().getPayload();
 *      BlockingDeviceOperator device = ctx.getDevice();
 *      //首次通信,进行认证
 *      if(device == null){
 *          String token = device.getConfigNow("token");
 *
 *      }else{
 *
 *      }
 *
 *  }
 *
 *  }</pre>
 *
 * @author zhouhao
 * @since 1.2.3
 */
public class BlockingMessageDecodeContext extends BlockingMessageCodecContext<MessageDecodeContext> {

    public BlockingMessageDecodeContext(Monitor monitor,
                                        MessageDecodeContext context,
                                        Duration timeout,
                                        ContextView ctx) {
        super(monitor, context, timeout, ctx);
    }

    /**
     * 获取当前上下文的设备操作接口,如果设备为首次连接,将会返回<code>null</code>.
     *
     * @return 设备操作接口
     */
    @Nullable
    @Override
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
     * 获取设备上行的原始消息
     *
     * @return EncodedMessage
     */
    public EncodedMessage getData() {
        return context.getMessage();
    }

    /**
     * 获取设备会话信息
     *
     * @return DeviceSession
     */
    public DeviceSession getSession() {
        return context.unwrap(FromDeviceMessageContext.class).getSession();
    }

    /**
     * 断开设备连接
     */
    public void disconnect() {
        getSession().close();
    }

    /**
     * 在处理完成后断开设备连接
     */
    public void disconnectLater() {
        async(Mono.fromRunnable(this::disconnect));
    }

    /**
     * 获取设备的连接信息,在长连接的场景下,可通过此方式获取到原始链接信息.
     *
     * @return ClientConnection
     * @see org.jetlinks.core.server.mqtt.MqttClientConnection
     */
    @Nullable
    public ClientConnection getConnection() {
        return context.unwrap(FromDeviceMessageContext.class).getConnection();
    }

    /**
     * 发送数据到设备
     * <p>
     * 注意: 避免在响应式上下文中执行此方法，否则可能导致性能问题。
     * 如果需要在响应式中使用，建议使用{@link #sendToDeviceReactive(EncodedMessage)}
     *
     * @param message 消息
     */
    public void sendToDeviceNow(EncodedMessage message) {
        await(
            context.unwrap(FromDeviceMessageContext.class)
                   .getSession()
                   .send(message)
        );
    }

    /**
     * 异步发送消息到设备
     *
     * @param message 消息
     */
    public void sendToDeviceLater(EncodedMessage message) {
        async(
            context.unwrap(FromDeviceMessageContext.class)
                   .getSession()
                   .send(message)
        );
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
     * 立即发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     * <p>
     * 注意: 避免在响应式上下文中执行此方法，否则可能导致性能问题。
     * 如果需要在响应式中使用，建议使用{@link #sendToPlatformReactive(DeviceMessage)}
     *
     * @param message 消息
     */
    public void sendToPlatformNow(DeviceMessage message) {
        if (message == null) {
            return;
        }
        await(context.handleMessage(message));
    }

    /**
     * 异步发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     *
     * @param message 消息
     */
    public void sendToPlatformLater(DeviceMessage message) {
        if (message == null) {
            return;
        }
        async(context.handleMessage(message));
    }

    /**
     * 响应式发送设备消息到平台,请使用平台内置的{@link DeviceMessage}相关,请勿自己在协议包中创建实现类.
     *
     * @param message 消息
     */
    public Mono<Void> sendToPlatformReactive(DeviceMessage message) {
        if (message == null) {
            return Mono.empty();
        }
        return context.handleMessage(message);
    }

}
