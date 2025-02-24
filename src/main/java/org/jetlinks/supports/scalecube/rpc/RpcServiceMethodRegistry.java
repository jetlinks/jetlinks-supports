package org.jetlinks.supports.scalecube.rpc;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

class RpcServiceMethodRegistry implements ServiceMethodRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMethodRegistry.class);

    private final List<ServiceInfo> serviceInfos = new CopyOnWriteArrayList<>();

    private final ConcurrentMap<String, ServiceMethodInvoker> methodInvokers =
        new ConcurrentHashMap<>();

    Disposable registerService0(ServiceInfo serviceInfo) {
        Disposable.Composite disposable = Disposables.composite();

        serviceInfos.add(serviceInfo);
        String serviceId = serviceInfo
            .tags()
            .getOrDefault(ScalecubeRpcManager.SERVICE_ID_TAG, ScalecubeRpcManager.DEFAULT_SERVICE_ID);

        Reflect.serviceInterfaces(serviceInfo.serviceInstance())
               .forEach(serviceInterface -> Reflect
                   .serviceMethods(serviceInterface)
                   .forEach(
                       (key, method) -> {

                           // validate method
                           Reflect.validateMethodOrThrow(method);
                           String serviceName = ScalecubeRpcManager.createMethodQualifier(serviceId, Reflect.serviceName(serviceInterface));

                           MethodInfo methodInfo =
                               new MethodInfo(
                                   serviceName,
                                   Reflect.methodName(method),
                                   Reflect.parameterizedReturnType(method),
                                   Reflect.isReturnTypeServiceMessage(method),
                                   Reflect.communicationMode(method),
                                   method.getParameterCount(),
                                   Reflect.requestType(method),
                                   Reflect.isRequestTypeServiceMessage(method),
                                   Reflect.isSecured(method));

                           checkMethodInvokerDoesntExist(methodInfo);

                           ServiceMethodInvoker methodInvoker =
                               new ServiceMethodInvoker(
                                   method,
                                   serviceInfo.serviceInstance(),
                                   methodInfo,
                                   serviceInfo.errorMapper(),
                                   serviceInfo.dataDecoder(),
                                   serviceInfo.authenticator(),
                                   serviceInfo.principalMapper());

                           methodInvokers.put(methodInfo.qualifier(), methodInvoker);
                           methodInvokers.put(methodInfo.oldQualifier(), methodInvoker);
                           disposable.add(() -> {
                               methodInvokers.remove(methodInfo.qualifier(), methodInvoker);
                               methodInvokers.remove(methodInfo.oldQualifier(), methodInvoker);
                           });
                       }));

        disposable.add(() -> serviceInfos.remove(serviceInfo));
        return disposable;
    }

    @Override
    public void registerService(ServiceInfo serviceInfo) {
        registerService0(serviceInfo);
    }

    private void checkMethodInvokerDoesntExist(MethodInfo methodInfo) {
        if (methodInvokers.containsKey(methodInfo.qualifier())
            || methodInvokers.containsKey(methodInfo.oldQualifier())) {
            LOGGER.error("MethodInvoker already exists, methodInfo: {}", methodInfo);
            throw new IllegalStateException("MethodInvoker already exists");
        }
    }

    @Override
    public ServiceMethodInvoker getInvoker(String qualifier) {
        return methodInvokers.get(Objects.requireNonNull(qualifier, "[getInvoker] qualifier"));
    }

    @Override
    public List<ServiceMethodInvoker> listInvokers() {
        return new ArrayList<>(methodInvokers.values());
    }

    @Override
    public List<ServiceInfo> listServices() {
        return new ArrayList<>(serviceInfos);
    }
}
