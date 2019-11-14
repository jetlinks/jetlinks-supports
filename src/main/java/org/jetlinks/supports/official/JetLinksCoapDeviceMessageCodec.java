package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.jetlinks.core.Value;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.supports.official.cipher.Ciphers;
import reactor.core.publisher.Mono;

@Slf4j
public class JetLinksCoapDeviceMessageCodec extends JetlinksTopicMessageCodec implements DeviceMessageCodec {


    @Override
    public Transport getSupportTransport() {
        return DefaultTransport.CoAP;
    }

    protected JSONObject decode(String text) {
        return JSON.parseObject(text);
    }

    @Override
    public Mono<? extends Message> decode(MessageDecodeContext context) {
        return Mono.defer(() -> {
            CoapMessage message = ((CoapMessage) context.getMessage());
            CoapExchange exchange = message.getExchange();
            String path = exchange.getRequestOptions().getUriString();


            return context.getDevice()
                    .getConfigs("encAlg","secureKey")
                    .flatMap(configs -> {
                        Ciphers ciphers = configs.getValue("encAlg").map(Value::asString).flatMap(Ciphers::of).orElse(Ciphers.AES);

                        String secureKey = configs.getValue("secureKey").map(Value::asString).orElse(null);

                        String payload = new String(ciphers.decrypt(exchange.getRequestPayload(),secureKey));
                        //解码
                        return Mono.just(decode(path, decode(payload)).getMessage());
                    })
                    .doOnSuccess(msg -> {
                        exchange.respond(CoAP.ResponseCode.CREATED);
                        exchange.accept();
                    })
                    .switchIfEmpty(Mono.fromRunnable(() -> {
                        exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                    }))
                    .doOnError(error -> {
                        log.error("decode coap message error", error);
                        exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                    });
        });
    }

    @Override
    public Mono<? extends EncodedMessage> encode(MessageEncodeContext context) {
        return Mono.empty();
    }
}
