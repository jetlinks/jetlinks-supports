package org.jetlinks.supports.protocol.management;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.protocol.StaticProtocolSupports;

import java.util.function.Consumer;

@Slf4j
@Setter
public class ManagementProtocolSupports extends StaticProtocolSupports {

    private ProtocolSupportManager manager;

    private ProtocolSupportLoader loader;

    private ClusterManager clusterManager;

    public void init() {
        manager.loadAll()
                .filter(de->de.getState()==1)
                .subscribe(this::init);

        clusterManager.<ProtocolSupportDefinition>getTopic("_protocol_changed")
                .subscribe()
                .subscribe(this::init);
    }

    public void init(ProtocolSupportDefinition definition) {
        String operation = definition.getState() !=1 ? "uninstall" : "install";
        Consumer<ProtocolSupport> consumer = definition.getState() !=1 ? this::unRegister : this::register;

        log.debug("{} protocol:{}", operation, definition);
        try {
            loader.load(definition)
                    .doOnError(e -> log.error("{} protocol[{}] error: {}", operation, definition.getId(), e))
                    .doOnNext(e -> log.debug("{} protocol[{}] success: {}", operation, definition.getId(), e))
                    .subscribe(consumer);
        } catch (Exception e) {
            log.error("load protocol error:{}", definition, e);
        }
    }

}
