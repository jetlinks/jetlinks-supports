package org.jetlinks.supports.official;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.Option;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.hswebframework.web.id.IDGenerator;
import org.jetlinks.core.Value;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.springframework.util.StringUtils;
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
            String _sign = null;
            String _token = null;
            for (Option other : exchange.getRequestOptions().getOthers()) {
                if (other.getNumber() == 2110) {
                    _sign = other.getStringValue();
                } else if (other.getNumber() == 2111) {
                    _token = other.getStringValue();
                }
            }
            String sign = _sign;
            String token = _token;

            if (StringUtils.isEmpty(sign)) {
                exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                return Mono.empty();
            }
            String payload = exchange.getRequestText();

            if ("/auth".equals(path)) {
                //认证
                return context.getDevice()
                        .getConfig("secureKey")
                        .flatMap(sk -> {
                            String secureKey = sk.asString();
                            if (!verifySign(secureKey, context.getDevice().getDeviceId(), payload, sign)) {
                                exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                                exchange.reject();
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
            } else {
                if (StringUtils.isEmpty(token)) {
                    exchange.respond(CoAP.ResponseCode.UNAUTHORIZED);
                    return Mono.empty();
                }
                return context.getDevice()
                        .getConfigs("secureKey", "coap-token")
                        .flatMap(configs -> {
                            String tk = configs.getValue("coap-token").map(Value::asString).orElse(null);
                            String secureKey = configs.getValue("secureKey").map(Value::asString).orElse(null);
                            if (!token.equals(tk)) {
                                exchange.respond(CoAP.ResponseCode.UNAUTHORIZED);
                                return Mono.empty();
                            }
                            //验证签名
                            if (!verifySign(secureKey, context.getDevice().getDeviceId(), payload, sign)) {
                                exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                                exchange.reject();
                                return Mono.empty();
                            }
                            //解码
                            return Mono.just(decode(path, decode(payload)).getMessage());
                        })
                        .doOnSuccess(msg -> {
                            exchange.respond(CoAP.ResponseCode.CREATED);
                            exchange.accept();
                        })
                        .doOnError(error -> {
                            log.error("decode coap message error", error);
                            exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                        });
            }
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
