package org.jetlinks.supports.scalecube.rpc;

import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.services.*;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import io.scalecube.services.transport.api.ServiceTransport;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.ServiceEvent;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class ScalecubeRpcManager implements RpcManager {
    private final String id = UUID.randomUUID().toString();

    private static final String SPREAD_ENDPOINT_QUALIFIER = "rpc_edp";

    private static final String SPREAD_FROM_HEADER = "rpc_edp_f";

    private final ExtendedCluster cluster;

    private ServiceCall serviceCall;

    private final Supplier<ServiceTransport> transportSupplier;

    // <serverNodeId,<qualifier,ServiceReference>>
    private final Map<String, Map<String, ServiceReference>> serverServiceRef = new NonBlockingHashMap<>();

    // <serverNodeId,<Type,Service>>
    private final Map<String, Map<Class<?>, Object>> serverServices = new NonBlockingHashMap<>();

    private final Map<String, Sinks.Many<ServiceEvent>> listener = new NonBlockingHashMap<>();
    private final List<ServiceRegistration> localRegistrations = new CopyOnWriteArrayList<>();

    private final ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();

    private ServiceTransport transport;

    private ServerTransport serverTransport;

    private String externalHost;

    private Integer externalPort;


    private Disposable syncJob = Disposables.disposed();

    public ScalecubeRpcManager() {
        this(null, null);
    }

    public ScalecubeRpcManager(ExtendedCluster cluster,
                               Supplier<ServiceTransport> transport) {
        this.cluster = cluster;
        this.transportSupplier = transport;
    }

    public ScalecubeRpcManager externalHost(String host) {
        ScalecubeRpcManager m = new ScalecubeRpcManager(cluster, transportSupplier);
        m.externalHost = host;
        return m;
    }

    public ScalecubeRpcManager externalPort(int port) {
        ScalecubeRpcManager m = new ScalecubeRpcManager(cluster, transportSupplier);
        m.externalPort = port;
        return m;
    }

    public ScalecubeRpcManager transport(Supplier<ServiceTransport> transportSupplier) {
        return new ScalecubeRpcManager(cluster, transportSupplier);
    }

    public ScalecubeRpcManager cluster(ExtendedCluster cluster) {
        return new ScalecubeRpcManager(cluster, transportSupplier);
    }


    public void startAwait() {

        startAsync().block();
    }

    public Mono<Void> startAsync() {
        Objects.requireNonNull(transportSupplier);
        Objects.requireNonNull(cluster);
        cluster.handler(ignore -> new ClusterMessageHandler() {

            @Override
            public void onMessage(Message message) {
                String from = message.header(SPREAD_FROM_HEADER);
                if (StringUtils.hasText(from) && SPREAD_ENDPOINT_QUALIFIER.equals(message.qualifier())) {
                    cluster.member(from)
                           .ifPresent(member -> handleServiceEndpoint(member, message.data()));
                }
            }

            @Override
            public void onGossip(Message gossip) {
                onMessage(gossip);
            }

            @Override
            public void onMembershipEvent(MembershipEvent event) {
                if (event.isLeaving() || event.isRemoved()) {
                    memberLeave(event.member());
                }
                if (event.isAdded() || event.isUpdated()) {
                    syncRegistration(event.member());
                }
            }
        });
        return this
                .transportSupplier
                .get()
                .start()
                .doOnNext(trans -> this.transport = trans)
                .flatMap(trans -> trans.serverTransport(methodRegistry).bind())
                .doOnNext(trans -> this.serverTransport = trans)
                .then(Mono.fromRunnable(this::start0));
    }

    private void start0() {
        this.serviceCall = new ServiceCall().transport(this.transport.clientTransport());
        syncRegistration();
    }

    public void stopAwait() {
        stopAsync().block();
    }

    public Mono<Void> stopAsync() {
        if (serverTransport == null || transport == null) {
            return Mono.empty();
        }
        localRegistrations.clear();
        syncRegistration();
        return Flux
                .concatDelayError(
                        doSyncRegistration().onErrorResume(err -> Mono.empty()),
                        serverTransport.stop(),
                        transport.stop()
                ).then();
    }


    private Address resolveAddress() {
        if (StringUtils.hasText(externalHost)) {
            return Address.create(externalHost, externalPort);
        }

        return prepareAddress(serverTransport.address());
    }

    private static Address prepareAddress(Address address) {
        final InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(address.host());
        } catch (UnknownHostException e) {
            throw Exceptions.propagate(e);
        }
        if (inetAddress.isAnyLocalAddress()) {
            return Address.create(Address.getLocalIpAddress().getHostAddress(), address.port());
        } else {
            return Address.create(inetAddress.getHostAddress(), address.port());
        }
    }

    private ServiceEndpoint createEndpoint() {
        return ServiceEndpoint
                .builder()
                .id(id)
                .address(resolveAddress())
                .contentTypes(DataCodec.getAllContentTypes())
                .serviceRegistrations(localRegistrations)
                .build();
    }

    private synchronized void syncRegistration(Member member) {
        cluster
                .send(member, Message
                        .withData(createEndpoint())
                        .header(SPREAD_FROM_HEADER, cluster.member().id())
                        .qualifier(SPREAD_ENDPOINT_QUALIFIER)
                        .build())
                .subscribe();
    }

    private Mono<Void> doSyncRegistration() {
        ServiceEndpoint endpoint = createEndpoint();
        log.debug("Synchronization registration : {}", endpoint);
        return cluster
                .spreadGossip(Message
                                      .withData(endpoint)
                                      .header(SPREAD_FROM_HEADER, cluster.member().id())
                                      .qualifier(SPREAD_ENDPOINT_QUALIFIER)
                                      .build())
                .then();
    }

    private synchronized void syncRegistration() {
        if (cluster == null) {
            return;
        }
        if (!syncJob.isDisposed()) {
            syncJob.dispose();
        }
        syncJob = Mono
                .delay(Duration.ofMillis(200))
                .flatMap(ignore -> doSyncRegistration())
                .subscribe();
    }

    @Override
    public <T> Disposable registerService(String service, T api) {
        ServiceInfo serviceInfo = ServiceInfo
                .fromServiceInstance(api)
                .errorMapper(DefaultErrorMapper.INSTANCE)
                .dataDecoder(ServiceMessageDataDecoder.INSTANCE)
                .build();

        methodRegistry.registerService(serviceInfo);

        List<ServiceRegistration> registrations = ServiceScanner
                .scanServiceInfo(serviceInfo)
                .stream()
                .map(ref -> {
                    Map<String, String> tags = new HashMap<>(ref.tags());
                    tags.put("s", service);
                    return new ServiceRegistration(service, tags, ref.methods());
                })
                .collect(Collectors.toList());

        localRegistrations.addAll(registrations);
        syncRegistration();
        log.debug("register rpc service {}", serviceInfo);
        return () -> {
            localRegistrations.removeAll(registrations);
            syncRegistration();
        };
    }

    @Override
    public <T> Disposable registerService(T api) {
        return registerService(Reflect.serviceName(api.getClass()), api);
    }

    private <API> API createApiCall(String serverId, Class<API> clazz) {
        return serviceCall
                .router((serviceRegistry, request) -> {
                    String qualifier = request.qualifier();
                    Map<String, ServiceReference> refs = serverServiceRef.get(serverId);
                    if (refs == null) {
                        return Optional.empty();
                    }
                    return Optional.ofNullable(refs.get(qualifier));
                })
                .serviceRegistry(NoneServiceRegistry.INSTANCE)
                .api(clazz);
    }

    @Override
    public <API> Flux<API> getServices(Class<API> service) {
        String name = Reflect.serviceName(service);

        return Flux
                .fromIterable(serverServiceRef.entrySet())
                .flatMap(e -> {
                    String serviceNode = e.getKey();
                    return Flux
                            .fromIterable(e.getValue().entrySet())
                            .any(ref -> ref.getKey().startsWith(name + "/"))
                            .filter(Boolean::booleanValue)
                            .flatMap(ignore -> getService(serviceNode, service));
                });
    }

    @Override
    @SuppressWarnings("all")
    public <API> Mono<API> getService(String serverNodeId,
                                      Class<API> service) {

        return Mono.just(
                (API) serverServices
                        .computeIfAbsent(serverNodeId, id -> new NonBlockingHashMap<>())
                        .computeIfAbsent(service, type -> createApiCall(serverNodeId, type))
        );
    }

    @Override
    public <API> Flux<ServiceEvent> listen(Class<API> service) {
        String name = Reflect.serviceName(service);
        return listener
                .computeIfAbsent(name, ignore -> Sinks.many().multicast().onBackpressureBuffer())
                .asFlux();
    }


    private void memberLeave(Member member) {
        String id = member.alias() == null ? member.id() : member.alias();
        Map<String, ServiceReference> ref = serverServiceRef.remove(id);
        if (null != ref) {
            fireEvent(ref.values()
                         .stream()
                         .map(ServiceReference::namespace)
                         .collect(Collectors.toSet()),
                      member.id(), ServiceEvent.Type.removed);
        }
        log.debug("remove service endpoint [{}] ", member);
    }

    private void fireEvent(Set<String> services, String memberId, ServiceEvent.Type type) {
        for (String service : services) {
            Sinks.Many<ServiceEvent> sink = listener.get(service);
            if (sink.currentSubscriberCount() > 0) {
                sink.tryEmitNext(new ServiceEvent(service, memberId, type));
            }
        }
    }

    private void handleServiceEndpoint(Member member, ServiceEndpoint endpoint) {
        if (cluster.member().id().equals(member.id())) {
            return;
        }
        String id = member.alias() == null ? member.id() : member.alias();

        Map<String, ServiceReference> references = serverServiceRef.computeIfAbsent(id, ignore -> new NonBlockingHashMap<>());
        List<String> readyToRemove = new ArrayList<>(references.keySet());
        Set<String> added = new HashSet<>();
        Set<String> removed = references
                .values()
                .stream()
                .map(ServiceReference::namespace)
                .collect(Collectors.toSet());

        log.debug("update service endpoint from [{}] : {} ", member, endpoint);
        endpoint
                .serviceReferences()
                .forEach(ref -> {
                    readyToRemove.remove(ref.qualifier());
                    readyToRemove.remove(ref.oldQualifier());
                    if (references.put(ref.qualifier(), ref) == null) {
                        added.add(ref.namespace());
                    }
                    references.put(ref.oldQualifier(), ref);

                    removed.remove(ref.namespace());
                });

        for (String qualifier : readyToRemove) {
            references.remove(qualifier);
        }

        fireEvent(added, id, ServiceEvent.Type.added);
        fireEvent(removed, id, ServiceEvent.Type.removed);


    }
}
