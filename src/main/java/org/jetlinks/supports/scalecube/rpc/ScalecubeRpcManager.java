package org.jetlinks.supports.scalecube.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.ThreadLocalRandom;
import io.rsocket.RSocketErrorException;
import io.rsocket.exceptions.ApplicationErrorException;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.services.*;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import io.scalecube.services.transport.api.ServiceTransport;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashSet;
import org.jetlinks.core.rpc.RpcManager;
import org.jetlinks.core.rpc.RpcService;
import org.jetlinks.core.rpc.ServiceEvent;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.reactivestreams.Publisher;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class ScalecubeRpcManager implements RpcManager {
    private final String id = UUID.randomUUID().toString();

    private static final String SPREAD_ENDPOINT_QUALIFIER = "rpc_edp";

    private static final String SPREAD_FROM_HEADER = "rpc_edp_f";

    static final String DEFAULT_SERVICE_ID = "_default";

    static final String SERVICE_ID_TAG = "_sid";
    static final String SERVICE_NAME_TAG = "_sname";
    static final String REGISTER_TIME_TAG = "_regtime";

    private ExtendedCluster cluster;

    private ServiceCall serviceCall;

    private static final RetryBackoffSpec DEFAULT_RETRY = Retry
        .backoff(12, Duration.ofMillis(100))
        .filter(err -> hasException(err,
                                    TimeoutException.class,
                                    SocketException.class,
                                    SocketTimeoutException.class,
                                    io.netty.handler.timeout.TimeoutException.class,
                                    IOException.class,
                                    RSocketErrorException.class))
        .doBeforeRetry(retrySignal -> {
            if (retrySignal.totalRetriesInARow() > 3) {
                log.warn("rpc retries {} : [{}]",
                         retrySignal
                             .retryContextView()
                             .<Method>getOrEmpty(Method.class)
                             .map(m -> m.getDeclaringClass().getName() + "." + m.getName())
                             .orElse("unknown"),
                         retrySignal.totalRetriesInARow(),
                         retrySignal.failure());
            }
        });

    @Setter
    @Getter
    private Retry retry = DEFAULT_RETRY;

    private Supplier<ServiceTransport> transportSupplier;

    private final Map<String, ClusterNode> serverServiceRef = new NonBlockingHashMap<>();

    private final Map<String, Sinks.Many<ServiceEvent>> listener = new NonBlockingHashMap<>();
    private final List<ServiceRegistration> localRegistrations = new CopyOnWriteArrayList<>();

    private final ServiceMethodRegistry methodRegistry = new RpcServiceMethodRegistry();

    private ServiceTransport transport;

    private ServerTransport serverTransport;

    private String externalHost;

    private Integer externalPort;

    private String contentType = ServiceMessage.DEFAULT_DATA_FORMAT;

    private Disposable syncJob = Disposables.disposed();

    private final Disposable.Composite disposable = Disposables.composite();

    public ScalecubeRpcManager() {
        this(null, null);
    }

    public ScalecubeRpcManager(ExtendedCluster cluster,
                               Supplier<ServiceTransport> transport) {
        this.cluster = cluster;
        this.transportSupplier = transport;
    }

    public ScalecubeRpcManager(ScalecubeRpcManager another) {
        this.cluster = another.cluster;
        this.transportSupplier = another.transportSupplier;
        this.externalHost = another.externalHost;
        this.externalPort = another.externalPort;
        this.contentType = another.contentType;
    }

    @Override
    public String currentServerId() {
        String alias = cluster.member().alias();
        String id = cluster.member().id();
        return alias == null ? id : alias;
    }

    /**
     * 设置对外访问的Host,通常是内网ip地址,并返回新的manager
     *
     * @param host Host
     * @return 新的manager
     */
    public ScalecubeRpcManager externalHost(String host) {
        ScalecubeRpcManager m = new ScalecubeRpcManager(this);
        m.externalHost = host;
        return m;
    }

    /**
     * 设置对外访问的port,与{@link ServiceTransport#serverTransport(ServiceMethodRegistry)}对应绑定的端口
     *
     * @param port 端口号
     * @return 新的manager
     */
    public ScalecubeRpcManager externalPort(Integer port) {
        ScalecubeRpcManager m = new ScalecubeRpcManager(this);
        m.externalPort = port;
        return m;
    }

    public ScalecubeRpcManager transport(Supplier<ServiceTransport> transportSupplier) {
        ScalecubeRpcManager m = new ScalecubeRpcManager(this);
        m.transportSupplier = transportSupplier;
        return m;
    }

    public ScalecubeRpcManager cluster(ExtendedCluster cluster) {
        ScalecubeRpcManager m = new ScalecubeRpcManager(this);
        m.cluster = cluster;
        return m;
    }

    public ScalecubeRpcManager contentType(String contentType) {
        ScalecubeRpcManager m = new ScalecubeRpcManager(this);
        m.contentType = contentType;
        return m;
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

        return initTransport(this.transportSupplier.get())
            .start()
            .doOnNext(trans -> this.transport = trans)
            .flatMap(trans -> trans.serverTransport(methodRegistry).bind())
            .doOnNext(trans -> this.serverTransport = trans)
            .then(Mono.fromRunnable(this::start0));
    }

    private ServiceTransport initTransport(ServiceTransport transport) {
        return transport;
    }

    @Override
    public Flux<RpcService<?>> getServices() {
        return Flux
            .fromIterable(serverServiceRef.values())
            .flatMapIterable(node -> node.serviceInstances.values())
            .flatMapIterable(Map::values);
    }

    private void start0() {
        this.serviceCall = new ServiceCall()
            .transport(this.transport.clientTransport());
        syncRegistration();
        disposable.add(Flux
                           .interval(Duration.ofSeconds(60))
                           .onBackpressureDrop()
                           .concatMap(ignore -> doSyncRegistration().onErrorResume(err -> Mono.empty()))
                           .subscribe());
    }

    public void stopAwait() {
        stopAsync().block();
    }

    public Mono<Void> stopAsync() {
        if (serverTransport == null || transport == null) {
            return Mono.empty();
        }
        localRegistrations.clear();
        disposable.dispose();
        return Flux
            .concatDelayError(
                doSyncRegistration().onErrorResume(err -> Mono.empty()),
                serverTransport.stop(),
                transport.stop()
            )
            .doOnComplete(() -> {
                serverTransport = null;
                transport = null;
            })
            .then();
    }

    private Address resolveAddress() {
        if (StringUtils.hasText(externalHost)) {
            if (externalPort != null) {
                return Address.create(externalHost, externalPort);
            }
            return Address.create(externalHost, serverTransport.address().port());
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
            .retryWhen(Retry.backoff(12, Duration.ofMillis(200)))
            .subscribe(ignore -> {
                       },
                       error -> log.error("Synchronization registration [{}] error", member, error));
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
            .doOnError(err -> log.error("Synchronization registration error", err))
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
    public <T> Disposable registerService(String service, T rpcService) {
        ServiceInfo serviceInfo = ServiceInfo
            .fromServiceInstance(rpcService)
            .errorMapper(DefaultErrorMapper.INSTANCE)
            .dataDecoder((msg, type) -> {
                if (type.isAssignableFrom(ByteBuf.class) && msg.hasData(ByteBuf.class)) {
                    return ServiceMessage
                        .from(msg)
                        .data(msg.data())
                        .build();
                }
                return ServiceMessageDataDecoder.INSTANCE.apply(msg, type);
            })
            .tag(SERVICE_ID_TAG, service)
            .build();

        methodRegistry.registerService(serviceInfo);

        List<ServiceRegistration> registrations = ServiceScanner
            .scanServiceInfo(serviceInfo)
            .stream()
            .map(ref -> {
                Map<String, String> tags = new HashMap<>(ref.tags());
                tags.put(SERVICE_ID_TAG, service);
                tags.put(SERVICE_NAME_TAG, ref.namespace());
                tags.put(REGISTER_TIME_TAG, String.valueOf(System.currentTimeMillis()));
                return new ServiceRegistration(createMethodQualifier(service, ref.namespace()), tags, ref.methods());
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
    public <T> Disposable registerService(T rpcService) {
        return registerService(DEFAULT_SERVICE_ID, rpcService);
    }

    @Override
    public <I> Flux<RpcService<I>> getServices(Class<I> service) {
        return Flux
            .fromIterable(serverServiceRef.entrySet())
            .flatMapIterable(e -> e.getValue().getApiCalls(service));
    }

    @Override
    public <I> Mono<RpcService<I>> selectService(Class<I> service) {
        List<RpcServiceCall<I>> calls = new ArrayList<>(serverServiceRef.size());

        for (Map.Entry<String, ClusterNode> entry : serverServiceRef.entrySet()) {
            calls.addAll(entry.getValue().getApiCalls(service));
        }
        if (calls.isEmpty()) {
            return Mono.empty();
        }
        if (calls.size() == 1) {
            return Mono.just(calls.get(0));
        }
        return Mono.just(calls.get(ThreadLocalRandom.current().nextInt(calls.size())));
    }

    @Override
    public <I> Flux<RpcService<I>> getServices(String id, Class<I> service) {
        return Flux
            .fromIterable(serverServiceRef.entrySet())
            .flatMapIterable(e -> e.getValue().getApiCalls(id, service));
    }

    @Override
    public <I> Mono<I> getService(String serverNodeId,
                                  Class<I> service) {
        return getService(serverNodeId, DEFAULT_SERVICE_ID, service);
    }

    @Override
    @SuppressWarnings("all")
    public <I> Mono<I> getService(String serverNodeId,
                                  String serviceId,
                                  Class<I> service) {

        return Mono.fromSupplier(() -> {
            ClusterNode node = serverServiceRef.get(serverNodeId);
            if (node == null) {
                return null;
            }
            return node
                .getApiCall(serviceId, service)
                .service();
        });
    }

    @Override
    public <I> Flux<ServiceEvent> listen(Class<I> service) {
        String name = Reflect.serviceName(service);
        return listener
            .computeIfAbsent(name, ignore -> Sinks.many().multicast().onBackpressureBuffer())
            .asFlux();
    }


    private void memberLeave(Member member) {
        String id = member.alias() == null ? member.id() : member.alias();
        ClusterNode ref = serverServiceRef.remove(id);
        if (null != ref) {
            fireEvent(ref.services, id, ServiceEvent.Type.removed);
        }
        log.debug("remove service endpoint [{}] ", member);
    }

    private void fireEvent(Collection<ServiceRegistration> services, String memberId, ServiceEvent.Type type) {
        for (ServiceRegistration service : services) {
            String serviceName = service.tags().getOrDefault(SERVICE_NAME_TAG, service.namespace());
            Sinks.Many<ServiceEvent> sink = listener.get(serviceName);
            if (sink != null && sink.currentSubscriberCount() > 0) {
                String id = service.tags().getOrDefault(SERVICE_ID_TAG, DEFAULT_SERVICE_ID);
                sink.emitNext(new ServiceEvent(id, serviceName, memberId, type), Reactors.emitFailureHandler());
            }
        }
    }

    private void handleServiceEndpoint(Member member, ServiceEndpoint endpoint) {
        if (cluster.member().id().equals(member.id())) {
            return;
        }
        String id = member.alias() == null ? member.id() : member.alias();

        ClusterNode references = serverServiceRef.computeIfAbsent(id, ignore -> new ClusterNode());
        references.id = id;
        references.member = member;
        references.rpcAddress = endpoint.address();
        references.register(endpoint);
    }


    static String createMethodQualifier(String serviceId, String qualifier) {
        return Qualifier.asString(serviceId, qualifier);
    }


    @SafeVarargs
    @SuppressWarnings("all")
    private static boolean hasException(Throwable e, Class<? extends Throwable>... target) {
        Throwable cause = e;
        while (cause != null) {
            for (Class<? extends Throwable> aClass : target) {
                if (aClass.isInstance(cause)) {
                    return true;
                }
                for (Throwable throwable : cause.getSuppressed()) {
                    if (throwable == e) {
                        continue;
                    }
                    boolean hasError = hasException(throwable, target);
                    if (hasError) {
                        return true;
                    }
                }
            }
            if (cause == cause.getCause()) {
                break;
            }
            cause = cause.getCause();
        }
        return false;
    }


    class ClusterNode {
        private String id;

        private Member member;

        private Address rpcAddress;

        private final Map<String, Set<ServiceReferenceInfo>> serviceReferencesByQualifier = new NonBlockingHashMap<>();

        private final List<ServiceRegistration> services = new CopyOnWriteArrayList<>();
        private final Map<Class<?>, Map<String, RpcServiceCall<?>>> serviceInstances = new NonBlockingHashMap<>();


        public synchronized void register(ServiceEndpoint endpoint) {
            List<String> readyToRemove = new ArrayList<>(serviceReferencesByQualifier.keySet());

            Set<ServiceRegistration> added = new TreeSet<>(Comparator.comparing(ServiceRegistration::namespace));
            Set<ServiceRegistration> removed = new TreeSet<>(Comparator.comparing(ServiceRegistration::namespace));
            removed.addAll(services);

            log.debug("update service endpoint from [{}] : {} ", member, endpoint);

            for (ServiceRegistration registration : endpoint.serviceRegistrations()) {
                if (!removed.remove(registration)) {
                    added.add(registration);
                }

                for (ServiceMethodDefinition method : registration.methods()) {
                    ServiceReference ref = new ServiceReference(method, registration, endpoint);

                    readyToRemove.remove(ref.qualifier());
                    readyToRemove.remove(ref.oldQualifier());
                    populateServiceReferences(ref.qualifier(), ref);
                    populateServiceReferences(ref.oldQualifier(), ref);

                }
            }

            for (String qualifier : readyToRemove) {
                serviceReferencesByQualifier.remove(qualifier);
            }

            removed.forEach(services::remove);
            services.addAll(added);

            fireEvent(added, id, ServiceEvent.Type.added);
            fireEvent(removed, id, ServiceEvent.Type.removed);

        }

        private boolean populateServiceReferences(String qualifier, ServiceReference serviceReference) {
            String id = serviceReference
                .tags()
                .getOrDefault(SERVICE_ID_TAG, DEFAULT_SERVICE_ID);

            return serviceReferencesByQualifier
                .computeIfAbsent(qualifier, key -> new NonBlockingHashSet<>())
                .add(new ServiceReferenceInfo(id, serviceReference));
        }

        private <I> RpcServiceCall<I> createApiCall(String serviceId, Class<I> clazz) {
            String name = Reflect.serviceName(clazz);

            ServiceCall call = serviceCall
                .router((serviceRegistry, request) -> {
                    Set<ServiceReferenceInfo> refs = serviceReferencesByQualifier.get(request.qualifier());
                    if (refs == null) {
                        return Optional.empty();
                    }
                    for (ServiceReferenceInfo ref : refs) {
                        return Optional.of(ref.reference);
                    }
                    return Optional.empty();
                })
                .serviceRegistry(NoneServiceRegistry.INSTANCE);

            return new RpcServiceCall<>(this.id, serviceId, name, api(call, serviceId, clazz));
        }

        private <I> List<RpcServiceCall<I>> getApiCalls(Class<I> clazz) {
            return getApiCalls(null, clazz);
        }

        private <I> List<RpcServiceCall<I>> getApiCalls(String id, Class<I> clazz) {
            String sName = Reflect.serviceName(clazz);
            List<RpcServiceCall<I>> registrations = new ArrayList<>(1);
            for (ServiceRegistration service : services) {
                String name = service.tags().getOrDefault(SERVICE_NAME_TAG, service.namespace());
                String sid = service.tags().getOrDefault(SERVICE_ID_TAG, DEFAULT_SERVICE_ID);
                if (!Objects.equals(name, sName)) {
                    continue;
                }
                if (id != null && !Objects.equals(sid, id)) {
                    continue;
                }

                registrations.add(getApiCall(sid, clazz, service));
            }
            return registrations;
        }

        @SuppressWarnings("all")
        private <I> RpcServiceCall<I> getApiCall(String id, Class<I> clazz, ServiceRegistration registration) {
            return (RpcServiceCall<I>) serviceInstances
                .computeIfAbsent(clazz, type -> new NonBlockingHashMap<>())
                .computeIfAbsent(id, _id -> createApiCall(_id, clazz));
        }

        @SuppressWarnings("all")
        private <I> RpcServiceCall<I> getApiCall(String id, Class<I> clazz) {
            return getApiCall(id, clazz, null);
        }

        private Mono<ServiceMessage> toServiceMessage(MethodInfo methodInfo, Object request) {

            ServiceMessage.Builder builder;

            if (request instanceof ServiceMessage) {
                builder = ServiceMessage
                    .from((ServiceMessage) request)
                    .qualifier(methodInfo.qualifier());
            } else {
                builder = ServiceMessage
                    .builder()
                    .qualifier(methodInfo.qualifier())
                    .data(request)
                    .dataFormatIfAbsent(contentType);
            }

            return TraceHolder
                .writeContextTo(builder, (ServiceMessage.Builder::header))
                .map(ServiceMessage.Builder::build);

        }

        private Retry getRetry(Method method) {
            // TODO: 2023/10/12 自定义retry
            if (retry instanceof RetryBackoffSpec) {
                return ((RetryBackoffSpec) retry).withRetryContext(Context.of(Method.class, method));
            }
            return retry;
        }

        @SuppressWarnings("all")
        private <T> T api(ServiceCall serviceCall, String id, Class<T> serviceInterface) {

            final Map<Method, MethodInfo> genericReturnTypes = new HashMap<>(Reflect.methodsInfo(serviceInterface));
            for (Map.Entry<Method, MethodInfo> entry : genericReturnTypes.entrySet()) {
                MethodInfo old = entry.getValue();
                entry.setValue(
                    new MethodInfo(
                        Qualifier.asString(id, old.serviceName()),
                        old.methodName(),
                        old.parameterizedReturnType(),
                        old.isReturnTypeServiceMessage(),
                        old.communicationMode(),
                        old.parameterCount(),
                        old.requestType(),
                        old.isRequestTypeServiceMessage(),
                        old.isSecured()
                    )
                );
            }

            // noinspection unchecked,Convert2Lambda
            return (T)
                Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class[]{serviceInterface},
                    new InvocationHandler() {
                        @Override
                        public Object invoke(Object proxy, Method method, Object[] params) {
                            Optional<Object> check =
                                toStringOrEqualsOrHashCode(method.getName(), serviceInterface, params);
                            if (check.isPresent()) {
                                return check.get(); // toString, hashCode was invoked.
                            }
                            final MethodInfo methodInfo = genericReturnTypes.get(method);
                            final Type returnType = methodInfo.parameterizedReturnType();
                            final boolean isServiceMessage = methodInfo.isReturnTypeServiceMessage();

                            Object request = methodInfo.requestType() == Void.TYPE ? null : params[0];

                            switch (methodInfo.communicationMode()) {
                                case FIRE_AND_FORGET:
                                    return toServiceMessage(methodInfo, request)
                                        .flatMap(serviceCall::oneWay)
                                        .retryWhen(getRetry(method));

                                case REQUEST_RESPONSE:
                                    return toServiceMessage(methodInfo, request)
                                        .flatMap(msg -> serviceCall.requestOne(msg, returnType))
                                        .transform(asMono(isServiceMessage))
                                        .retryWhen(getRetry(method));

                                case REQUEST_STREAM:

                                    return toServiceMessage(methodInfo, request)
                                        .flatMapMany(msg -> serviceCall.requestMany(msg, returnType))
                                        .transform(asFlux(isServiceMessage))
                                        .retryWhen(getRetry(method));

                                case REQUEST_CHANNEL:
                                    // this is REQUEST_CHANNEL so it means params[0] must
                                    // be a publisher - its safe to cast.
                                    //noinspection rawtypes
                                    return serviceCall
                                        .requestBidirectional(
                                            Flux.from((Publisher) request)
                                                .flatMap(data -> toServiceMessage(methodInfo, data)),
                                            returnType)
                                        .transform(asFlux(isServiceMessage))
                                        .retryWhen(getRetry(method));

                                default:
                                    throw new IllegalArgumentException(
                                        "Communication mode is not supported: " + method);
                            }
                        }
                    });
        }


        private Function<Flux<ServiceMessage>, Flux<Object>> asFlux(boolean isReturnTypeServiceMessage) {
            return flux ->
                isReturnTypeServiceMessage ? flux.cast(Object.class) : flux.map(ServiceMessage::data);
        }

        private Function<Mono<ServiceMessage>, Mono<Object>> asMono(boolean isReturnTypeServiceMessage) {
            return mono ->
                isReturnTypeServiceMessage ? mono.cast(Object.class) : mono.map(ServiceMessage::data);
        }

        private Optional<Object> toStringOrEqualsOrHashCode(
            String method, Class<?> serviceInterface, Object... args) {

            switch (method) {
                case "toString":
                    return Optional.of(serviceInterface.toString());
                case "equals":
                    return Optional.of(serviceInterface.equals(args[0]));
                case "hashCode":
                    return Optional.of(serviceInterface.hashCode());

                default:
                    return Optional.empty();
            }
        }
    }

    @AllArgsConstructor
    static class RpcServiceCall<T> implements RpcService<T> {
        private final String serverNodeId;
        private final String id;
        private final String name;
        private final T service;

        @Override
        public String serverNodeId() {
            return serverNodeId;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public T service() {
            return service;
        }

        public <R> R cast(Class<R> type) {
            return type.cast(service);
        }
    }

    @EqualsAndHashCode(of = "id")
    @AllArgsConstructor
    static class ServiceReferenceInfo {
        private String id;
        private ServiceReference reference;
    }
}
