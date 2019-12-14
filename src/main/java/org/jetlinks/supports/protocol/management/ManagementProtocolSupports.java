package org.jetlinks.supports.protocol.management;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.protocol.StaticProtocolSupports;

@Slf4j
@Setter
public class ManagementProtocolSupports extends StaticProtocolSupports {

    private ProtocolSupportManager manager;

    private ProtocolSupportLoader loader;

    private ClusterManager clusterManager;

    public void init() {
        manager.loadAll()
                .filter(de -> de.getState() == 1)
                .subscribe(this::init);

        clusterManager.<ProtocolSupportDefinition>getTopic("_protocol_changed")
                .subscribe()
                .subscribe(this::init);
    }

    public void init(ProtocolSupportDefinition definition) {
        if (definition.getState() != 1) {
            log.debug("uninstall protocol:{}", definition);
            unRegister(definition.getId());
            return;
        }

        log.debug("install protocol:{}", definition);
        try {
            loader.load(definition)
                    .doOnError(e -> log.error("install protocol[{}] error: {}", definition.getId(), e))
                    .doOnNext(e -> log.debug("install protocol[{}] success: {}", definition.getId(), e))
                    .subscribe(this::register);
        } catch (Exception e) {
            log.error("load protocol error:{}", definition, e);
        }
    }

}
