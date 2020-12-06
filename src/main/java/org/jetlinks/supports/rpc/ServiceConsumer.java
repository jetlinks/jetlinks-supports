package org.jetlinks.supports.rpc;

import lombok.SneakyThrows;
import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.Codec;
import org.jetlinks.core.codec.Codecs;
import org.jetlinks.core.codec.defaults.DirectCodec;
import org.jetlinks.core.ipc.IpcDefinition;
import org.jetlinks.core.ipc.IpcInvoker;
import org.jetlinks.core.ipc.IpcInvokerBuilder;
import org.jetlinks.core.ipc.IpcService;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ResolvableType;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ServiceConsumer implements Disposable {

    private final Disposable disposable;

    private final Class<?> serviceInterface;

    private final Map<String, MethodInvoker> mappings = new HashMap<>();

    private final Logger log;

    ServiceConsumer(IpcService ipcService, String address, Object instance, Class<?> serviceInterface) {
        this.serviceInterface = serviceInterface;
        for (Method method : serviceInterface.getMethods()) {
            mappings.put(method.getName() + ":" + method.getParameterCount(), createInvoker(method, instance));
        }

        IpcDefinition<Payload, Payload> definition = IpcDefinition.of(address, DirectCodec.instance(), IpcRpcServiceFactory.responseCodec);
        this.disposable = ipcService.listen(definition, createInvoker());
        log = LoggerFactory.getLogger(serviceInterface);
    }


    private IpcInvoker<Payload, Payload> createInvoker() {

        return IpcInvokerBuilder.<Payload, Payload>newBuilder()
                .name(serviceInterface.getName())
                .forRequest(request -> Mono.from(invoke(request)))
                .forRequestStream(request -> Flux.from(invoke(request)))
                .forFireAndForget(request -> Mono.from(invoke(request)).then())
                .build();
    }

    protected static List<Codec<Object>> createMethodArgsCodec(Method method) {
        List<ResolvableType> argTypes = new ArrayList<>();
        for (int i = 0; i < method.getParameterCount(); i++) {
            ResolvableType resolvableType = ResolvableType.forMethodParameter(method, i);
            if (resolvableType.isAssignableFrom(Publisher.class)) {
                throw new UnsupportedOperationException("unsupported publisher arg type :" + method);
            }
            argTypes.add(resolvableType);
        }
        return argTypes.stream().map(Codecs::lookup).collect(Collectors.toList());
    }

    private MethodInvoker getInvoker(String method, int argCount) {
        return mappings.get(method + ":" + argCount);
    }

    private Publisher<Payload> invoke(Payload request) {

        MethodInvoker[] temp = new MethodInvoker[1];

        MethodRequest methodRequest = MethodRequestCodec
                .decode(request, (methodName, argCount) -> (temp[0] = getInvoker(methodName, argCount))
                        .getRequestCodecs());
        if (log.isDebugEnabled()) {
            if (methodRequest.getArgs() != null && methodRequest.getArgs().length > 0) {
                log.debug("invoke local service: {}({})", methodRequest.getMethod(), methodRequest.getArgs());
            } else {
                log.debug("invoke local service: {}()", methodRequest.getMethod());
            }
        }

        return temp[0].invoke(methodRequest);
    }

    private MethodInvoker createInvoker(Method method, Object instance) {
        return new MethodInvoker(instance, method);
    }

    static class MethodInvoker {
        final Method method;
        final Object instance;
        final Codec<Object> responseEncoder;
        final List<Codec<Object>> requestCodecs;

        public MethodInvoker(Object instance, Method method) {
            this.instance = instance;
            this.method = method;
            this.responseEncoder = Codecs.lookup(ResolvableType.forMethodReturnType(method));
            this.requestCodecs = createMethodArgsCodec(method);
        }

        public List<Codec<Object>> getRequestCodecs() {
            return requestCodecs;
        }

        @SneakyThrows
        Publisher<Payload> invoke(MethodRequest request) {
            Object res = method.invoke(instance, request.getArgs());
            if (res instanceof Mono) {
                return ((Mono<?>) res)
                        .map(responseEncoder::encode);
            } else if (res instanceof Flux) {
                return ((Flux<?>) res)
                        .map(responseEncoder::encode);
            }
            return Mono.empty();
        }
    }


    @Override
    public void dispose() {
        disposable.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposable.isDisposed();
    }
}
