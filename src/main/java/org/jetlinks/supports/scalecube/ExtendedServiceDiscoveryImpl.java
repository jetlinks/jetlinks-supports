package org.jetlinks.supports.scalecube;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryContext;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import org.jetlinks.core.utils.Reactors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointAdded;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointLeaving;

public final class ExtendedServiceDiscoveryImpl implements ServiceDiscovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

    private ClusterConfig clusterConfig;
    private ExtendedCluster cluster;

    private ServiceEndpoint endpoint;
    // Sink
    private final Sinks.Many<ServiceDiscoveryEvent> sink =
            Sinks.many().multicast().directBestEffort();

    /**
     * Constructor.
     */
    public ExtendedServiceDiscoveryImpl() {
        this.clusterConfig = ClusterConfig.defaultLanConfig();
    }

    public ExtendedServiceDiscoveryImpl(ExtendedCluster other, ServiceEndpoint endpoint) {
        this();
        this.cluster = other;
        this.endpoint = endpoint;
    }

    /**
     * Copy constructor.
     *
     * @param other other instance
     */
    private ExtendedServiceDiscoveryImpl(ExtendedServiceDiscoveryImpl other) {
        this.clusterConfig = other.clusterConfig;
        this.cluster = other.cluster;
        this.endpoint = other.endpoint;
    }

    /**
     * Setter for {@code ClusterConfig} options.
     *
     * @param opts options operator
     * @return new instance of {@code ScalecubeServiceDiscovery}
     */
    public ExtendedServiceDiscoveryImpl options(UnaryOperator<ClusterConfig> opts) {
        ExtendedServiceDiscoveryImpl d = new ExtendedServiceDiscoveryImpl(this);
        d.clusterConfig = opts.apply(clusterConfig);
        return d;
    }

    /**
     * Setter for {@code TransportConfig} options.
     *
     * @param opts options operator
     * @return new instance of {@code ScalecubeServiceDiscovery}
     */
    public ExtendedServiceDiscoveryImpl transport(UnaryOperator<TransportConfig> opts) {
        return options(cfg -> cfg.transport(opts));
    }

    /**
     * Setter for {@code MembershipConfig} options.
     *
     * @param opts options operator
     * @return new instance of {@code ScalecubeServiceDiscovery}
     */
    public ExtendedServiceDiscoveryImpl membership(UnaryOperator<MembershipConfig> opts) {
        return options(cfg -> cfg.membership(opts));
    }

    /**
     * Setter for {@code GossipConfig} options.
     *
     * @param opts options operator
     * @return new instance of {@code ScalecubeServiceDiscovery}
     */
    public ExtendedServiceDiscoveryImpl gossip(UnaryOperator<GossipConfig> opts) {
        return options(cfg -> cfg.gossip(opts));
    }

    /**
     * Setter for {@code FailureDetectorConfig} options.
     *
     * @param opts options operator
     * @return new instance of {@code ScalecubeServiceDiscovery}
     */
    public ExtendedServiceDiscoveryImpl failureDetector(UnaryOperator<FailureDetectorConfig> opts) {
        return options(cfg -> cfg.failureDetector(opts));
    }

    private Mono<ExtendedCluster> initCluster() {
        if (cluster == null) {
            cluster = new ExtendedClusterImpl(clusterConfig.metadata(endpoint));
            return ((ExtendedClusterImpl) cluster).start();
        } else {
            return Mono.just(cluster);
        }
    }

    /**
     * Starts scalecube service discovery. Joins a cluster with local services as metadata.
     *
     * @return mono result
     */
    @Override
    public Mono<Void> start() {
        return Mono
                .deferContextual(
                        context -> {
                            ServiceDiscoveryContext.Builder discoveryContextBuilder =
                                    context.get(ServiceDiscoveryContext.Builder.class);
                            // Start scalecube-cluster and listen membership events
                            return initCluster()
                                    .doOnNext(cluster1 -> cluster1.handler(
                                            cluster -> new ClusterMessageHandler() {
                                                @Override
                                                public void onMembershipEvent(MembershipEvent event) {
                                                    ExtendedServiceDiscoveryImpl.this.onMembershipEvent(event);
                                                }

                                            }))
                                    .flatMap(cluster -> cluster.updateMetadata(endpoint).thenReturn(cluster))
                                    .doOnSuccess(cluster -> discoveryContextBuilder.address(cluster.address()))
                                    .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
                                    .then(loadMembers());
                        });
    }

    private Mono<Void> loadMembers() {
        return Flux.fromIterable(cluster.otherMembers())
                   .flatMap(member -> Mono.justOrEmpty(cluster.metadata(member)))
                   .doOnNext(metadata -> {
                       if (metadata instanceof ServiceEndpoint) {
                           sink.emitNext(newEndpointAdded(((ServiceEndpoint) metadata)), Reactors.emitFailureHandler());
                       }
                   })
                   .then();
    }

    @Override
    public Flux<ServiceDiscoveryEvent> listen() {
        return sink.asFlux().onBackpressureBuffer();
    }

    @Override
    public Mono<Void> shutdown() {
        return Mono.defer(
                () -> {
                    if (cluster == null) {
                        sink.emitComplete(Reactors.emitFailureHandler());
                        return Mono.empty();
                    }
                    cluster.shutdown();
                    return cluster.onShutdown().doFinally(s -> sink.emitComplete(Reactors.emitFailureHandler()));
                });
    }

    private void onMembershipEvent(MembershipEvent membershipEvent) {
        LOGGER.debug("onMembershipEvent: {}", membershipEvent);

        ServiceDiscoveryEvent discoveryEvent = toServiceDiscoveryEvent(membershipEvent);
        if (discoveryEvent == null) {
            LOGGER.debug(
                    "DiscoveryEvent is null, cannot publish it (corresponding membershipEvent: {})",
                    membershipEvent);
            return;
        }

        LOGGER.debug("Publish discoveryEvent: {}", discoveryEvent);
        sink.emitNext(discoveryEvent, Reactors.emitFailureHandler());
    }

    private ServiceDiscoveryEvent toServiceDiscoveryEvent(MembershipEvent membershipEvent) {
        ServiceDiscoveryEvent discoveryEvent = null;

        if (membershipEvent.isAdded() && membershipEvent.newMetadata() != null) {
            ServiceEndpoint serviceEndpoint = decodeMetadata(membershipEvent.newMetadata());
            discoveryEvent = serviceEndpoint == null ? null : newEndpointAdded(serviceEndpoint);
        }
        if (membershipEvent.isUpdated() && membershipEvent.newMetadata() != null) {
            discoveryEvent = newEndpointAdded(decodeMetadata(membershipEvent.newMetadata()));
        }
        if (membershipEvent.isRemoved() && membershipEvent.oldMetadata() != null) {
            ServiceEndpoint serviceEndpoint = decodeMetadata(membershipEvent.oldMetadata());
            discoveryEvent = serviceEndpoint == null ? null : newEndpointLeaving(serviceEndpoint);
        }
        if (membershipEvent.isLeaving() && membershipEvent.newMetadata() != null) {
            ServiceEndpoint serviceEndpoint = decodeMetadata(membershipEvent.newMetadata());

            discoveryEvent = serviceEndpoint == null ? null : newEndpointLeaving(serviceEndpoint);
        }

        return discoveryEvent;
    }

    private ServiceEndpoint decodeMetadata(ByteBuffer byteBuffer) {
        try {
            return (ServiceEndpoint) clusterConfig.metadataCodec().deserialize(byteBuffer.duplicate());
        } catch (Exception e) {
            LOGGER.error("Failed to read metadata: " + e);
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ExtendedServiceDiscoveryImpl.class.getSimpleName() + "[", "]")
                .add("cluster=" + cluster)
                .add("clusterConfig=" + clusterConfig)
                .toString();
    }

    public ExtendedServiceDiscoveryImpl updateEndpoint(ServiceEndpoint endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    @SuppressWarnings("unused")
    public interface MonitorMBean {

        String getClusterConfig();

        String getRecentDiscoveryEvents();
    }

    private static class JmxMonitorMBean implements MonitorMBean {

        private static final String OBJECT_NAME_FORMAT = "io.scalecube.services.discovery:name=%s@%s";

        public static final int RECENT_DISCOVERY_EVENTS_SIZE = 128;

        private final ExtendedServiceDiscoveryImpl discovery;
        private final List<ServiceDiscoveryEvent> recentDiscoveryEvents = new CopyOnWriteArrayList<>();

        private JmxMonitorMBean(ExtendedServiceDiscoveryImpl discovery) {
            this.discovery = discovery;
        }

        private static JmxMonitorMBean start(ExtendedServiceDiscoveryImpl instance) throws Exception {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            JmxMonitorMBean jmxMBean = new JmxMonitorMBean(instance);
            jmxMBean.init();
            ObjectName objectName =
                    new ObjectName(
                            String.format(OBJECT_NAME_FORMAT, instance.cluster.member().id(), System.nanoTime()));
            StandardMBean standardMBean = new StandardMBean(jmxMBean, MonitorMBean.class);
            mbeanServer.registerMBean(standardMBean, objectName);
            return jmxMBean;
        }

        private void init() {
            discovery.listen().subscribe(this::onDiscoveryEvent);
        }

        @Override
        public String getClusterConfig() {
            return String.valueOf(discovery.clusterConfig);
        }

        @Override
        public String getRecentDiscoveryEvents() {
            return recentDiscoveryEvents.stream()
                                        .map(ServiceDiscoveryEvent::toString)
                                        .collect(Collectors.joining(",", "[", "]"));
        }

        private void onDiscoveryEvent(ServiceDiscoveryEvent event) {
            recentDiscoveryEvents.add(event);
            if (recentDiscoveryEvents.size() > RECENT_DISCOVERY_EVENTS_SIZE) {
                recentDiscoveryEvents.remove(0);
            }
        }
    }
}
