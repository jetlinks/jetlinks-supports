package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.Option;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

@Slf4j
public class JetLinksCoapDTLSDeviceMessageCodec extends JetlinksTopicMessageCodec implements DeviceMessageCodec {

    @Override
    public Transport getSupportTransport() {
        return DefaultTransport.CoAP_DTLS;
    }

    @Override
    public Mono<? extends Message> decode(MessageDecodeContext context) {
        return Mono.defer(() -> {
            CoapMessage message = ((CoapMessage) context.getMessage());
            CoapExchange exchange = message.getExchange();
            String path = exchange.getRequestOptions().getUriString();
            String _sign = null;
            String _token = null;
            for (Option other : exchange.getRequestOptions().getOthers()) {
                if (other.getNumber() == 2110) {
                    _sign = other.getStringValue();
                }
                if (other.getNumber() == 2111) {
                    _token = other.getStringValue();
                }
            }

            if ("/auth".equals(path)) {
                String sign = _sign;
                //认证
                return context.getDevice()
                        .getConfig("secureKey")
                        .flatMap(sk -> {
                            String secureKey = sk.asString();
                            if (!verifySign(secureKey, context.getDevice().getDeviceId(), exchange.getRequestText(), sign)) {
                                exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                                return Mono.empty();
                            }
                            String newToken = IDGenerator.MD5.generate();
                            return context.getDevice()
                                    .setConfig("coap-token", newToken)
                                    .doOnSuccess(success -> {
                                        JSONObject json = new JSONObject();
                                        json.put("token", newToken);
                                        exchange.respond(json.toJSONString());
                                        exchange.accept();
                                    });
                        })
                        .then(Mono.empty());
            }
            String token = _token;
            if (StringUtils.isEmpty(token)) {
                exchange.respond(CoAP.ResponseCode.UNAUTHORIZED);
                return Mono.empty();
            }
            return
                    context.getDevice()
                            .getConfig("coap-token")
                            .switchIfEmpty(Mono.fromRunnable(() -> {
                                exchange.respond(CoAP.ResponseCode.UNAUTHORIZED);
                            }))
                            .flatMap(value -> {
                                String tk = value.asString();
                                if (!token.equals(tk)) {
                                    exchange.respond(CoAP.ResponseCode.UNAUTHORIZED);
                                    return Mono.empty();
                                }
                                return Mono
                                        .just(decode(path, JSON.parseObject(exchange.getRequestText())).getMessage())
                                        .switchIfEmpty(Mono.fromRunnable(() -> {
                                            exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                                        }));
                            })
                            .doOnSuccess(msg -> {
                                exchange.respond(CoAP.ResponseCode.CREATED);
                                exchange.accept();
                            })
                            .doOnError(error -> {
                                log.error("decode coap message error", error);
                                exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                            });
        });
    }

    protected boolean verifySign(String secureKey, String deviceId, String payload, String sign) {
        //验证签名
        if (StringUtils.isEmpty(secureKey) || !DigestUtils.md5Hex(payload.concat(secureKey)).equalsIgnoreCase(sign)) {
            log.info("device [{}] coap sign [{}] error, payload:{}", deviceId, sign, payload);
            return false;
        }
        return true;
    }

    @Override
    public Mono<? extends EncodedMessage> encode(MessageEncodeContext context) {
        return Mono.empty();
    }
}
