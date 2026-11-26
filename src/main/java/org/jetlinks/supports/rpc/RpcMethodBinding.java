package org.jetlinks.supports.rpc;

import org.jetlinks.core.rpc.annotation.*;
import org.jetlinks.core.lang.SharedPathString;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.lang.invoke.*;
import java.lang.reflect.*;
import java.util.*;

/** 方法签名与 invoker 只在绑定时解析，调用时不再查找反射元数据。 */
final class RpcMethodBinding {
    static final ClassValue<Api> APIS = new ClassValue<>() {
        protected Api computeValue(Class<?> type) { return new Api(type); }
    };
    final String name;
    final Type resultType;
    final Class<?> requestType;
    final Type requestDataType;
    final boolean channel, stream, forget;
    final SharedPathString clientSpan, serverSpan;
    private final MethodHandle clientInvoker;

    RpcMethodBinding(Method method, String apiName) {
        RpcMethod annotation = method.getAnnotation(RpcMethod.class);
        name = annotation == null || annotation.value().isEmpty() ? method.getName() : annotation.value();
        clientSpan = SharedPathString.of("/_rpc/client/" + apiName + "/" + name);
        serverSpan = SharedPathString.of("/_rpc/server/" + apiName + "/" + name);
        if (method.getParameterCount() > 1 || !(method.getGenericReturnType() instanceof ParameterizedType result)
            || !(method.getReturnType() == Mono.class || method.getReturnType() == Flux.class)) {
            throw new IllegalArgumentException("RPC methods require Mono/Flux and at most one argument: " + method);
        }
        requestType = method.getParameterCount() == 0 ? Void.TYPE : method.getParameterTypes()[0];
        channel = Publisher.class.isAssignableFrom(requestType);
        requestDataType = channel ? ((ParameterizedType) method.getGenericParameterTypes()[0]).getActualTypeArguments()[0]
            : method.getParameterCount() == 0 ? Void.TYPE : method.getGenericParameterTypes()[0];
        stream = method.getReturnType() == Flux.class;
        forget = annotation != null && annotation.fireAndForget();
        if (channel && !stream || forget && (stream || result.getActualTypeArguments()[0] != Void.class)) {
            throw new IllegalArgumentException("Unsupported RPC interaction: " + method);
        }
        resultType = result.getActualTypeArguments()[0];
        try {
            MethodHandle handle = MethodHandles.publicLookup().unreflect(method);
            if (requestType == Void.TYPE) handle = MethodHandles.dropArguments(handle, 1, Object.class);
            clientInvoker = handle.asType(MethodType.methodType(Publisher.class, Object.class, Object.class));
        } catch (IllegalAccessException error) { throw new IllegalArgumentException("RPC API must be public", error); }
    }

    MethodHandle bind(Object service) {
        return clientInvoker.bindTo(service);
    }
    Publisher<?> invokeClient(Object client, Object request) {
        try { return (Publisher<?>) clientInvoker.invokeExact(client, request); }
        catch (Throwable error) { return Mono.error(error); }
    }

    static final class Api {
        final String name;
        final Map<Method, RpcMethodBinding> methods;
        final Map<String, RpcMethodBinding> byName;
        Api(Class<?> type) {
            if (!type.isInterface()) throw new IllegalArgumentException("RPC API must be an interface: " + type);
            RpcApi annotation = type.getAnnotation(RpcApi.class);
            if (annotation == null) throw new IllegalArgumentException("RPC interface requires @RpcApi: " + type);
            name = annotation.value().isEmpty() ? type.getName() : annotation.value();
            Map<Method, RpcMethodBinding> definitions = new HashMap<>();
            Map<String, RpcMethodBinding> names = new HashMap<>();
            for (Method method : type.getMethods()) {
                if (Modifier.isStatic(method.getModifiers())) continue;
                RpcMethodBinding binding = new RpcMethodBinding(method, name);
                if (names.putIfAbsent(binding.name, binding) != null) throw new IllegalArgumentException("Duplicate RPC method name: " + binding.name);
                definitions.put(method, binding);
            }
            methods = Map.copyOf(definitions);
            byName = Map.copyOf(names);
        }
    }
}
