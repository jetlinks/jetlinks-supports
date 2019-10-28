package org.jetlinks.supports.protocol.script;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.*;
import org.jetlinks.core.metadata.DeviceMetadataCodec;
import org.jetlinks.supports.protocol.Authenticator;
import org.jetlinks.supports.protocol.CompositeProtocolSupport;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class ScriptProtocolSupport extends CompositeProtocolSupport {

    private String id;
    private String name;
    private String description;

    private ScriptProtocolInfo script;
    private DeviceMetadataCodec metadataCodec;
    private ScriptEvaluator scriptEvaluator;

    private Transport transport;

    @SneakyThrows
    public void init() {
        String authenticator = "protocol.auth(function(request,device){" +
                "\n" +
                script.getAuthenticator() +
                "\n})";
        transport = () -> script.getTransport();
        Map<String, Object> scriptContext = new HashMap<>();
        scriptContext.put("protocol", this);

        String messageEncoder = "protocol.codec(function(context){" +
                "\nvar message=context.getMessage();" +
                "\nvar device =context.getDevice();" +
                "\n" +
                script.getMessageEncoder() +
                "\n" +
                "}," +
                "\nfunction(context){" +
                "\nvar message=context.getMessage();" +
                "\nvar device =context.getDevice();" +
                "\n" +
                script.getMessageDecoder() +
                "\n}\n" +
                ")}";

        scriptEvaluator.evaluate(script.getLang(), authenticator, scriptContext);

        scriptEvaluator.evaluate(script.getLang(), messageEncoder, scriptContext);

    }

    public void auth(Authenticator authenticator) {
        addAuthenticator(transport, authenticator);
    }

    public void codec(DeviceMessageEncoder encoder, DeviceMessageDecoder deviceMessageDecoder) {

        addMessageCodecSupport(transport, () -> Mono.just(new DeviceMessageCodec() {
            @Override
            public Transport getSupportTransport() {
                return transport;
            }

            @Override
            public <T extends Message> Mono<T> decode(MessageDecodeContext context) {
                return deviceMessageDecoder.decode(context);
            }

            @Override
            public Mono<EncodedMessage> encode(MessageEncodeContext context) {
                return encoder.encode(context);
            }
        }));
    }


    @Nonnull
    @Override
    public DeviceMetadataCodec getMetadataCodec() {
        return metadataCodec;
    }


}
