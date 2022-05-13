package org.jetlinks.supports.scalecube.rpc;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.Collections;
import java.util.List;

public class NoneServiceRegistry implements ServiceRegistry {
    public static final ServiceRegistry INSTANCE = new NoneServiceRegistry();

    @Override
    public List<ServiceEndpoint> listServiceEndpoints() {
        return Collections.emptyList();
    }

    @Override
    public List<ServiceReference> listServiceReferences() {
        return Collections.emptyList();
    }

    @Override
    public List<ServiceReference> lookupService(ServiceMessage request) {
        return Collections.emptyList();
    }

    @Override
    public boolean registerService(ServiceEndpoint serviceEndpoint) {
        return false;
    }

    @Override
    public ServiceEndpoint unregisterService(String endpointId) {
        return null;
    }
}
