package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import org.jetlinks.coap.CoapPacket;
import org.jetlinks.coap.enums.Code;
import org.jetlinks.coap.enums.MediaTypes;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import reactor.core.publisher.Mono;

public class JetlinksCoapDeviceMessageCodec extends JetlinksTopicMessageCodec implements DeviceMessageCodec {
    @Override
    public Transport getSupportTransport() {
        return DefaultTransport.CoAP;
    }

    @Override
    public Mono<? extends Message> decode(MessageDecodeContext context) {
        return Mono.fromSupplier(() -> {
            CoAPMessage message = ((CoAPMessage) context.getMessage());
            CoapPacket packet = message.getPacket();
            String path = packet.headers().getUriPath();
            return decode(path, JSON.parseObject(new String(packet.getPayload()))).getMessage();
        });
    }

    @Override
    public Mono<? extends EncodedMessage> encode(MessageEncodeContext context) {
        return Mono.defer(() -> {
            Message message = context.getMessage();
            if (message instanceof DeviceMessage) {
                DeviceMessage deviceMessage = ((DeviceMessage) message);
                EncodedTopic convertResult = encode(deviceMessage.getDeviceId(), deviceMessage);
                CoapPacket packet=new CoapPacket();
                packet.headers().setUriPath(convertResult.topic);
                packet.headers().setContentFormat(MediaTypes.CT_APPLICATION_JSON);
                packet.setCode(Code.C205_CONTENT);
                packet.setPayload(JSON.toJSONBytes(convertResult.payload));
                return context.getDevice()
                        .getConfig(DeviceConfigKey.productId)
                        .map(productId -> new CoAPMessage(deviceMessage.getDeviceId(),packet));
            } else {
                return Mono.empty();
            }
        });
    }
}
