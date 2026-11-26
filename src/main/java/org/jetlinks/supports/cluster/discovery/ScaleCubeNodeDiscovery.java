package org.jetlinks.supports.cluster.discovery;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import org.jetlinks.core.cluster.discovery.*;
import org.jetlinks.supports.rpc.LatestValue;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.*;
import java.net.URI;
import java.util.*;

/** 拥有独立 ScaleCube 成员实例的发现适配器；不使用 ScaleCube Services。 */
public final class ScaleCubeNodeDiscovery implements NodeDiscovery {
    private static final Logger log = LoggerFactory.getLogger(ScaleCubeNodeDiscovery.class);
    private final ClusterConfig config;
    private final boolean owned;
    private final LatestValue<ClusterView> view = new LatestValue<>(new ClusterView(0, Map.of()));
    private final Map<String, NodeAnnouncement> members = new HashMap<>();
    private ExtendedCluster cluster;
    private NodeAnnouncement local;
    private Disposable listener;
    private boolean started, stopped;

    public ScaleCubeNodeDiscovery(ClusterConfig config) { this.config = config; owned = true; }
    /** 借用已有成员；调用方须保证本适配器独占其应用公告元数据。 */
    public ScaleCubeNodeDiscovery(ExtendedCluster cluster, ClusterConfig config) {
        this.cluster = cluster; this.config = config; owned = false;
    }

    @Override public Mono<Void> start(NodeAnnouncement announcement) {
        return Mono.defer(() -> {
            byte[] metadata;
            synchronized (this) {
                if (started || stopped) return Mono.error(new IllegalStateException("Discovery already started/stopped"));
                started = true;
                local = announcement;
                try {
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    NodeAnnouncementCodec.write(local, new DataOutputStream(bytes));
                    metadata = bytes.toByteArray();
                    if (owned) cluster = new ExtendedClusterImpl(config.memberAlias(local.identity().nodeId()).metadata(metadata));
                    listener = cluster.listenMembership().subscribe(this::onMember, view::fail);
                } catch (Exception error) { return Mono.error(error); }
            }
            Mono<?> ready = owned ? ((ExtendedClusterImpl) cluster).start() : cluster.updateMetadata(metadata);
            return ready.doOnSuccess(ignored -> {
                synchronized (this) {
                    members.put(cluster.member().id(), local);
                    for (Member member : cluster.otherMembers()) cluster.metadata(member).ifPresent(data -> put(member.id(), data));
                    publish();
                }
            }).then().onErrorResume(error -> stop().then(Mono.error(error)));
        });
    }

    private synchronized void onMember(MembershipEvent event) {
        if (stopped) return;
        if (event.isRemoved() || event.isLeaving()) members.remove(event.member().id());
        else if (event.newMetadata() != null) {
            try { put(event.member().id(), config.metadataCodec().deserialize(event.newMetadata().duplicate())); }
            catch (Exception error) {
                members.remove(event.member().id());
                log.warn("Invalid node announcement from {}", event.member().id(), error);
            }
        }
        publish();
    }

    private void put(String memberId, Object metadata) {
        if (!(metadata instanceof byte[] data) || data.length == 0) { members.remove(memberId); return; }
        try {
            NodeAnnouncement node = NodeAnnouncementCodec.read(new DataInputStream(new ByteArrayInputStream(data)));
            if (local.namespace().equals(node.namespace())) members.put(memberId, node);
            else members.remove(memberId);
        } catch (IOException error) { throw Exceptions.propagate(error); }
    }

    private void publish() {
        Map<NodeIdentity, NodeAnnouncement> snapshot = new LinkedHashMap<>();
        members.values().forEach(node -> snapshot.put(node.identity(), node));
        if (snapshot.equals(view.current().members())) return;
        view.update(new ClusterView(view.current().revision() + 1, snapshot));
    }
    public ClusterView currentView() { return view.current(); }
    /** 实际发现地址，供动态端口部署的 seed 配置和运行观测使用。 */
    public URI address() { return URI.create("tcp://" + cluster.address()); }
    public Flux<ClusterView> watch() { return view.watch(); }

    public Mono<Void> stop() {
        return Mono.defer(() -> {
            synchronized (this) {
                if (stopped) return Mono.empty();
                stopped = true;
                if (listener != null) listener.dispose();
                members.clear();
                publish();
                view.close();
                if (cluster == null) return Mono.empty();
                if (!owned) return started && !cluster.isShutdown() ? cluster.updateMetadata(new byte[0]) : Mono.empty();
                cluster.shutdown();
                return cluster.onShutdown();
            }
        });
    }
}
