package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.jetlinks.core.rpc.RpcBodyCodec;
import reactor.core.Exceptions;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;

/** 通用 Java 对象适配；生产可注入平台 codec，二进制请求不经过本 codec。 */
public final class JavaRpcBodyCodec implements RpcBodyCodec {
    public String id() { return "java/1"; }
    public void encode(Object value, ByteBuf target) {
        try (ObjectOutputStream output = new ObjectOutputStream(new ByteBufOutputStream(target))) {
            output.writeObject(value);
        } catch (Exception error) { throw Exceptions.propagate(error); }
    }
    public Object decode(ByteBuf source, Type type) {
        try (ObjectInputStream input = new ObjectInputStream(new ByteBufInputStream(source))) {
            return input.readObject();
        } catch (Exception error) { throw Exceptions.propagate(error); }
    }
}
