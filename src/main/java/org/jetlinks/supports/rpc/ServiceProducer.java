package org.jetlinks.supports.rpc;

import org.jetlinks.core.Payload;
import org.jetlinks.core.codec.Codec;
import org.jetlinks.core.codec.Codecs;
import org.jetlinks.core.codec.defaults.DirectCodec;
import org.jetlinks.core.ipc.IpcDefinition;
import org.jetlinks.core.ipc.IpcInvoker;
import org.jetlinks.core.ipc.IpcService;
import org.jetlinks.core.rpc.DisposableService;
import org.jetlinks.supports.ipc.RequestType;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ResolvableType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ServiceProducer<T> implements DisposableService<T> {

    private final Class<T> serviceInterface;

    private final T service;

    private final Logger log;

    @SuppressWarnings("all")
    public ServiceProducer(String address, IpcService ipcService, Class<T> serviceInterface) {
        this.serviceInterface = serviceInterface;
        Map<Method, Function<Object[], Object>> invokers = new HashMap<>();

        IpcInvoker<Payload, Payload> remote = ipcService.createInvoker(serviceInterface.getName(),
                                                                       IpcDefinition.of(address, DirectCodec.instance(), IpcRpcServiceFactory.responseCodec));

        for (Method method : serviceInterface.getMethods()) {
            invokers.put(method, createInvoker(remote, method));
        }
        this.log = LoggerFactory.getLogger(serviceInterface);

        InvocationHandler handler = (proxy, method, args) -> {
            Function<Object[], Object> invoker = invokers.get(method);
            log.debug("invoke method:{}", method.getName());
            return invoker.apply(args);
        };

        this.service = (T) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                                                  new Class[]{serviceInterface},
                                                  handler);

    }

    private Function<Object[], Object> createInvoker(IpcInvoker<Payload, Payload> remote, Method method) {
        ResolvableType returnType = ResolvableType.forMethodReturnType(method);
        List<ResolvableType> argTypes = new ArrayList<>();
        for (int i = 0; i < method.getParameterCount(); i++) {
            argTypes.add(ResolvableType.forMethodParameter(method, i));
        }
        String methodName = method.getName();
        List<Codec<?>> codecs = argTypes.stream().map(Codecs::lookup).collect(Collectors.toList());
        MethodRequestCodec codec = MethodRequestCodec.of(codecs);
        Codec<Object> responseCodec = Codecs.lookup(returnType);
        RequestType requestType = getRpcType(argTypes, returnType);

        switch (requestType) {
            case request:
                return args -> remote
                        .request(codec.encode(MethodRequest.of(methodName, args)))
                        .map(response -> response.decode(responseCodec));
            case noArgRequest:
                return args -> remote
                        .request(codec.encode(MethodRequest.of(methodName, null)))
                        .map(response -> response.decode(responseCodec));
            case requestStream:
                return (args) -> remote
                        .requestStream(codec.encode(MethodRequest.of(methodName, args)))
                        .map(response -> response.decode(responseCodec));
            case noArgRequestStream:
                return (args) -> remote
                        .requestStream(codec.encode(MethodRequest.of(methodName, null)))
                        .map(response -> response.decode(responseCodec));

        }

        throw new UnsupportedOperationException("unsupported rpc method:" + method);

    }

    protected RequestType getRpcType(List<ResolvableType> argTypes, ResolvableType returnType) {
        boolean hasArg = argTypes.size() > 0;
        Class<?> returnClass = returnType.toClass();

        boolean argHasStream = argTypes
                .stream()
                .anyMatch(type -> Publisher.class.isAssignableFrom(type.toClass()));

        if (hasArg && argHasStream) {
            throw new UnsupportedOperationException("unsupported publisher arg yet");
        }

        if (Mono.class.isAssignableFrom(returnClass)) {
            if (hasArg) {
                return RequestType.request;
            }
            return RequestType.noArgRequest;
        }

        if (Flux.class.isAssignableFrom(returnClass)) {
            if (hasArg) {
                return RequestType.requestStream;
            }
            return RequestType.noArgRequestStream;
        }
        return hasArg ? RequestType.fireAndForget : RequestType.noArgFireAndForget;
    }

    @Override
    public T getService() {
        return service;
    }

    @Override
    public void dispose() {

    }

    @Override
    public String toString() {
        return serviceInterface.toString();
    }
}
