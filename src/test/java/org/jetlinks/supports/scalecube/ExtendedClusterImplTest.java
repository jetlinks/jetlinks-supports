package org.jetlinks.supports.scalecube;


import io.scalecube.cluster.ClusterConfig;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ExtendedClusterImplTest {


    @Test
    @SneakyThrows
    public void testFeature() {
        ExtendedClusterImpl cluster = new ExtendedClusterImpl(
                ClusterConfig.defaultConfig()
                             .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
        );
        cluster.registerFeatures(Arrays.asList(
                "device-manager",
                "rule-engine"
        ));
        cluster.startAwait();

        ExtendedClusterImpl node2, node3, node4;
        {
            node2 = new ExtendedClusterImpl(
                    ClusterConfig.defaultConfig()
                                 .membership(conf -> conf.seedMembers(cluster.address()))
                                 .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
            );
            cluster.registerFeatures(Arrays.asList(
                    "device-manager",
                    "rule-engine"
            ));
            node2.startAwait();
        }

        {
            node3 = new ExtendedClusterImpl(
                    ClusterConfig.defaultConfig()
                                 .membership(conf -> conf.seedMembers(cluster.address()))
                                 .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
            );
            cluster.registerFeatures(Arrays.asList(
                    "device-manager"
            ));
            node3.startAwait();
        }

        {
            node4 = new ExtendedClusterImpl(
                    ClusterConfig.defaultConfig()
                                 .membership(conf -> conf.seedMembers(cluster.address()))
                                 .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
            );
            cluster.registerFeatures(Arrays.asList(
                    "rule-engine"
            ));
            node4.startAwait();
        }

        Assert.assertEquals(3, cluster.featureMembers("device-manager").size());
        Assert.assertEquals(3, cluster.featureMembers("rule-engine").size());

        node4.shutdown();
        node4.onShutdown().block();
        Assert.assertEquals(2, cluster.featureMembers("rule-engine").size());

    }
}