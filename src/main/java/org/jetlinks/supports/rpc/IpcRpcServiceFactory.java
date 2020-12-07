package org.jetlinks.supports.rpc;

import io.netty.util.ReferenceCountUtil;
import lombok.AllArgsConstructor;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.Codec;
import org.jetlinks.core.ipc.IpcService;
import org.jetlinks.core.rpc.DisposableService;
import org.jetlinks.core.rpc.RpcServiceFactory;
import reactor.core.Disposable;

import javax.annotation.Nonnull;

@AllArgsConstructor
public class IpcRpcServiceFactory implements RpcServiceFactory {

    private final IpcService ipcService;

    static ResponseCodec responseCodec = new ResponseCodec();

    static class ResponseCodec implements Codec<Payload> {

        @Override
        public Class<Payload> forType() {
            return Payload.class;
        }

        @Override
        public Payload decode(@Nonnull Payload payload) {
            try {
                return Payload.of(payload.getBody().copy());
            }finally {
                ReferenceCountUtil.safeRelease(payload);
            }
        }

        @Override
        public Payload encode(Payload body) {
            return body;
        }
    }

    @Override
    public <T> DisposableService<T> createProducer(String address, Class<T> serviceInterface) {
        return new ServiceProducer<>(address, ipcService, serviceInterface);
    }


    @Override
    public <T> Disposable createConsumer(String address, Class<T> serviceInterface, T instance) {
        return new ServiceConsumer(ipcService, address, instance, serviceInterface);
    }
}
