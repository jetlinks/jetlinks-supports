package org.jetlinks.supports.scalecube;

import io.scalecube.services.ServiceInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;

import java.util.Collections;
import java.util.List;

public class EmptyServiceMethodRegistry implements ServiceMethodRegistry {

    public static final ServiceMethodRegistry INSTANCE = new EmptyServiceMethodRegistry();

    private EmptyServiceMethodRegistry(){}
    @Override
    public void registerService(ServiceInfo serviceInfo) {

    }

    @Override
    public ServiceMethodInvoker getInvoker(String qualifier) {
        return null;
    }

    @Override
    public List<ServiceMethodInvoker> listInvokers() {
        return Collections.emptyList();
    }

    @Override
    public List<ServiceInfo> listServices() {
        return Collections.emptyList();
    }
}
