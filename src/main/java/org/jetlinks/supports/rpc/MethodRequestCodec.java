package org.jetlinks.supports.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.Codec;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiFunction;

public class MethodRequestCodec implements Codec<MethodRequest> {

    private final List<Codec<Object>> parameterCodecs;

    private MethodRequestCodec(List<Codec<Object>> parameterCodecs) {
        this.parameterCodecs = parameterCodecs;
    }

    public static MethodRequestCodec of(List<Codec<?>> parameterCodecs) {
        return new MethodRequestCodec((List) parameterCodecs);
    }


    @Override
    public Class<MethodRequest> forType() {
        return MethodRequest.class;
    }

    public static MethodRequest decode(Payload payload, BiFunction<String, Integer, List<Codec<Object>>> argCodecGetter) {
        try {
            ByteBuf body = payload.getBody();
            int index = body.readerIndex();

            int methodLen = body.getInt(index);
            index += 4;
            byte[] methodNameBytes = new byte[methodLen]; //方法名
            body.getBytes(index, methodNameBytes);
            index += methodLen;
            String methodName = new String(methodNameBytes);
            Object[] args = null;

            byte argCount = body.getByte(index++);//参数数量

            List<Codec<Object>> parameterCodecs = argCodecGetter.apply(methodName, (int) argCount);
            if (!parameterCodecs.isEmpty()) {
                int argSize = parameterCodecs.size();
                args = new Object[argSize];
                int argLen;
                for (int i = 0; i < argSize; i++) {
                    Codec<Object> codec = parameterCodecs.get(i);
                    argLen = body.getInt(index);
                    index += 4;

                    if (argLen >= 0) {
                        ByteBuf buf;
                        if (argLen == 0) {
                            buf = Unpooled.EMPTY_BUFFER;
                        } else {
                            buf = ByteBufAllocator.DEFAULT.buffer(argLen);
                            body.getBytes(index, buf);
                        }
                        args[i] = Payload.of(buf).decode(codec);
                        index += argLen;
                    }
                }
            }
            return MethodRequest.of(methodName, args);
        } finally {
            //ReferenceCountUtil.safeRelease(payload);
        }
    }

    @Override
    public MethodRequest decode(@Nonnull Payload payload) {
        return decode(payload, (methodName, size) -> parameterCodecs);
    }

    @Override
    public Payload encode(MethodRequest body) {
        Object[] args = body.getArgs();
        byte[] name = body.getMethod().getBytes();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        byteBuf.writeInt(name.length);
        byteBuf.writeBytes(name);
        byteBuf.writeByte(parameterCodecs.size());

        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                Codec<Object> encoder = parameterCodecs.get(i);
                Payload payload = encoder.encode(arg);
                try {
                    ByteBuf argBuf = payload.getBody();
                    byteBuf.writeInt(argBuf.writerIndex());
                    byteBuf.writeBytes(argBuf);
                } finally {
                    ReferenceCountUtil.safeRelease(payload);
                }
            }
        }
        return Payload.of(byteBuf);
    }
}
