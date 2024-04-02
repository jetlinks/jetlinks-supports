package org.jetlinks.supports.protocol.management;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.supports.protocol.StaticProtocolSupports;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 动态管理协议包支持. 实现集群时协议包动态管理.
 *
 * @see DefaultProtocolSupportManager
 * @deprecated {@link DefaultProtocolSupportManager}
 */
@Slf4j
@Setter
@Deprecated
public class ManagementProtocolSupports extends StaticProtocolSupports {

    private ProtocolSupportManager manager;

    private ProtocolSupportLoader loader;

    private ClusterManager clusterManager;

    private Map<String, String> configProtocolIdMapping = new ConcurrentHashMap<>();

    /**
     * 初始化
     *
     * <pre>
     *     1.加载全部协议
     *     2.订阅topic,来监听集群的协议操作通知
     * </pre>
     */
    public void init() {
        manager.loadAll()
               .filter(de -> de.getState() == 1)
               .subscribe(this::init);

        clusterManager.<ProtocolSupportDefinition>getTopic("_protocol_changed")
                      .subscribe()
                      .subscribe(this::init);
    }

    /**
     * 初始化协议
     * <p>
     * 如果{@link ProtocolSupportDefinition#getState()}不为1，则表示卸载协议。为0则表示加载协议.
     *
     * @param definition 协议定义
     */
    public void init(ProtocolSupportDefinition definition) {
        if (definition.getState() != 1) {
            String protocol = configProtocolIdMapping.get(definition.getId());
            if (protocol != null) {
                log.debug("uninstall protocol:{}", definition);
                unRegister(protocol);
                return;
            }
        }
        String operation = definition.getState() != 1 ? "uninstall" : "install";
        Consumer<ProtocolSupport> consumer = definition.getState() != 1 ? this::unRegister : this::register;

        log.debug("{} protocol:{}", operation, definition);
        try {
            loader.load(definition)
                  .doOnNext(protocolSupport -> protocolSupport.init(definition.getConfiguration()))
                  .doOnError(e -> log.error("{} protocol[{}] error: {}", operation, definition.getId(), e))
                  .doOnNext(e -> log.debug("{} protocol[{}] success: {}", operation, definition.getId(), e))
                  .doOnNext(e -> configProtocolIdMapping.put(definition.getId(), e.getId()))
                  .subscribe(consumer);
        } catch (Exception e) {
            log.error("load protocol error:{}", definition, e);
        }
    }

}
