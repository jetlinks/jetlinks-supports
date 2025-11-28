package org.jetlinks.supports.scalecube.rpc;

import com.fasterxml.jackson.core.JacksonException;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.ThreadLocalRandom;
import io.rsocket.exceptions.Retryable;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.services.*;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import io.scalecube.services.transport.api.ServiceTransport;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.recycler.Recycler;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashSet;
import org.jetlinks.core.rpc.*;
import org.jetlinks.core.trace.TraceHolder;
import org.jetlinks.core.utils.HashUtils;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.reactivestreams.Publisher;
import org.springframework.core.codec.CodecException;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Slf4j
public class ScalecubeRpcManager implements RpcManager {
    private final String id = UUID.randomUUID().toString();

    private static final String SPREAD_ENDPOINT_QUALIFIER = "rpc_edp";

    private static final String SPREAD_FROM_HEADER = "rpc_edp_f";

    private static final Recycler<List<RpcService<?>>> SHARED = Recycler.create(
        () -> new ArrayList<>(2), List::clear, 64);

    static final String DEFAULT_SERVICE_ID = "_default";

    static final String SERVICE_ID_TAG = "_sid";
    static final String SERVICE_NAME_TAG = "_sname";
    static final String REGISTER_TIME_TAG = "_regtime";

    public static final String HEADER_CONTEXT = ".ctx";

    private ExtendedCluster cluster;

    private ContextCodec contextCodec = ContextCodec.DEFAULT;

    private ServiceCall serviceCall;

    private Scheduler requestScheduler = Schedulers.parallel();

    private final DetailErrorMapper errorMapper = new DetailErrorMapper();

    static final RetryBackoffSpec DEFAULT_RETRY = Retry
        .backoff(12, Duration.ofMillis(50))
        .maxBackoff(Duration.ofSeconds(2))
        .jitter(0.2)
        .filter(err ->
                    !hasException(err,
                                  JacksonException.class,
                                  MessageCodecException.class,
                                  CodecException.class)
                        && hasException(
                        err,
                        Retryable.class,
                        ServiceUnavailableException.class,
                        TimeoutException.class,
                        SocketException.class,
                        SocketTimeoutException.class,
                        io.netty.handler.timeout.TimeoutException.class,
                        IOException.class
                    ))
        .scheduler(Schedulers.parallel())
        .onRetryExhaustedThrow((spec,signal)-> signal.failure())
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
            } else {
                log.info("rpc retries {} : [{}]",
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

    private final RpcServiceMethodRegistry methodRegistry = new RpcServiceMethodRegistry();

    private ServiceTransport transport;

    private ServerTransport serverTransport;

    private String externalHost;

    private Integer externalPort;

    private String contentType = ServiceMessage.DEFAULT_DATA_FORMAT;

    private final Disposable.Swap syncJob = Disposables.swap();

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

    public void setContextCodec(ContextCodec contextCodec) {
        this.contextCodec = contextCodec;
        methodRegistry.setContextCodec(contextCodec);
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
                    //尝试延迟重新同步一次
                    if (event.isLeaving()) {
                        Schedulers
                            .parallel()
                            .schedule(() -> {
                                if (cluster.member(event.member().id()).isPresent()) {
                                    syncRegistration(event.member());
                                }
                            }, 10, TimeUnit.SECONDS);
                    } else {
                        Disposable _old = syncMembers.remove(event.member());
                        if (_old != null) {
                            _old.dispose();
                        }
                    }
                }
                if (event.isAdded() || event.isUpdated()) {
                    Disposable _old = syncMembers.remove(event.member());
                    if (_old != null) {
                        _old.dispose();
                    }
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
            .flatMapIterable(ServiceInstances::getAllCalls);
    }

    @Override
    public boolean isShutdown() {
        return disposable.isDisposed() || (cluster != null && cluster.isShutdown());
    }


    private void start0() {

        StackTraceElement trace = new StackTraceElement(
            "RpcCallFailed",
            currentServerId(),
            null,
            1
        );

        errorMapper.setTopTrace(trace);

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
        syncJob.dispose();
        disposable.dispose();
        serverServiceRef.clear();
        localRegistrations.clear();
        return Flux
            .concatDelayError(
                doSyncRegistration(),
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

    private final Map<Member, Disposable> syncMembers = new ConcurrentHashMap<>();

    private synchronized void syncRegistration(Member member) {
        if (syncMembers.containsKey(member) || cluster.isShutdown()) {
            return;
        }
        Disposable.Swap _dispose = Disposables.swap();
        _dispose.update(
            Mono.defer(() -> cluster
                    .send(member, Message
                        .withData(createEndpoint())
                        .header(SPREAD_FROM_HEADER, cluster.member().id())
                        .qualifier(SPREAD_ENDPOINT_QUALIFIER)
                        .build()))
                .retryWhen(Retry
                               .fixedDelay(30, Duration.ofSeconds(1))
                               .filter(err -> err.getMessage() == null
                                   || err.getMessage().contains("Connection refused")
                                   || cluster.member(member.id()).isPresent()))
                .doFinally(ignore -> syncMembers.remove(member, _dispose))
                .subscribe(null,
                           error -> {
                               if (cluster.member(member.id()).isPresent()) {
                                   log.error("Synchronization registration [{}] error", member, error);
                               }
                           })
        );

        syncMembers.put(member, _dispose);

    }

    private Mono<Void> doSyncRegistration() {
        if (cluster.isShutdown()) {
            return Mono.empty();
        }
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
        syncJob.update(
            Mono
                .delay(Duration.ofMillis(200))
                .flatMap(ignore -> doSyncRegistration())
                .subscribe()
        );
    }

    @Override
    public <T> Disposable registerService(String service, T rpcService) {
        Disposable.Composite _dispose = Disposables.composite();
        ServiceInfo serviceInfo = ServiceInfo
            .fromServiceInstance(rpcService)
            .errorMapper(errorMapper)
            .dataDecoder((msg, type) -> {
                if (type == ByteBuf.class && msg.hasData(ByteBuf.class)) {
                    return ServiceMessage
                        .from(msg)
                        .data(msg.data())
                        .build();
                }
                return ServiceMessageDataDecoder.INSTANCE.apply(msg, type);
            })
            .tag(SERVICE_ID_TAG, service)
            .build();

        _dispose.add(methodRegistry.registerService0(serviceInfo));

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
            .toList();

        localRegistrations.addAll(registrations);
        syncRegistration();
        log.debug("register rpc service {}", serviceInfo);

        _dispose.add(() -> {
            localRegistrations.removeAll(registrations);
            syncRegistration();
        });

        return _dispose;
    }

    @Override
    public <T> Disposable registerService(T rpcService) {
        return registerService(DEFAULT_SERVICE_ID, rpcService);
    }

    @Override
    public <I> Flux<RpcService<I>> getServices(Class<I> service) {
        return Flux.defer(() -> Flux
            .fromIterable(serverServiceRef.entrySet())
            .flatMapIterable(e -> e.getValue().getApiCalls(service)));
    }

    @Override
    public <I> Mono<RpcService<I>> selectService(Class<I> service) {
        return selectService(service, (Object) null);
    }

    @SuppressWarnings("all")
    @Override
    public <T, I> Mono<I> selectService(Class<I> service,
                                        Collector<RpcService<I>, T, Optional<RpcService<I>>> selector,
                                        Mono<I> fallback) {
        I proxy = (I) Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[]{service},
            new InvocationHandler() {

                @SneakyThrows
                private <T> T invokeUnsafe(Object proxy, Method method, Object[] args) {
                    return (T) method.invoke(proxy, args);
                }

                private Mono<I> selectAsync() {
                    return Mono.defer(() -> {
                        RpcService<I> call = selectService0(service, selector).orElse(null);
                        if (call == null) {
                            return fallback;
                        } else {
                            return Mono.just(call.service());
                        }
                    });
                }

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    Retry _retry = getRetry(method);
                    if (Flux.class.isAssignableFrom(method.getReturnType())) {
                        return selectAsync()
                            .flatMapMany(service -> {
                                return invokeUnsafe(service, method, args);
                            })
                            .retryWhen(_retry)
                            .contextWrite(ctx -> ctx.put(Retry.class, _retry));
                    }
                    if (Mono.class.isAssignableFrom(method.getReturnType())) {
                        return selectAsync()
                            .flatMap(service -> {
                                return invokeUnsafe(service, method, args);
                            })
                            .retryWhen(_retry)
                            .contextWrite(ctx -> ctx.put(Retry.class, _retry));
                    }
                    RpcService<I> call = selectService0(service, selector).orElse(null);
                    if (call == null) {
                        return method.invoke(Reactors.await(fallback, Duration.ofSeconds(1)), args);
                    }
                    return method.invoke(call.service(), args);
                }
            }
        );

        return Mono.just(proxy);
    }

    private <T, I, R> R selectService0(Class<I> service,
                                       Collector<RpcService<I>, T, R> selector) {
        T container = selector.supplier().get();
        BiConsumer<T, RpcService<I>> acc = selector.accumulator();
        //查找所有的节点
        for (Map.Entry<String, ClusterNode> entry : serverServiceRef.entrySet()) {
            entry.getValue().getApiCalls(null, service, container, acc);
        }
        return selector.finisher().apply(container);
    }


    @Override
    public <I> Mono<RpcService<I>> selectService(Class<I> service, Object routeKey) {
        if (routeKey == null) {
            return Mono
                .fromSupplier(() -> this
                    .selectService0(
                        service,
                        Collectors.minBy(Comparator.comparingLong(s -> ThreadLocalRandom.current().nextInt())))
                    .orElse(null));
        }
        return Mono
            .fromSupplier(() -> this
                .selectService0(
                    service,
                    Collectors.minBy(Comparator.comparingLong(s -> hash(s.serverNodeId(), routeKey))))
                .orElse(null));
    }


    @Override
    public <I> Flux<RpcService<I>> getServices(String id, Class<I> service) {
        return Flux.defer(() -> Flux
            .fromIterable(serverServiceRef.entrySet())
            .flatMapIterable(e -> e.getValue().getApiCalls(id, service)));
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
            RpcServiceCall<I> call = node.getApiCall(serviceId, service);

            return node.isSupported(call) ? call.service() : null;
        });
    }

    @Override
    public <I> Flux<ServiceEvent> listen(Class<I> service) {
        String name = Reflect.serviceName(service);
        return listener
            .computeIfAbsent(name, ignore -> Sinks.many().multicast().onBackpressureBuffer())
            .asFlux();
    }

    private Retry getRetry(Method method) {
        // TODO: 2023/10/12 自定义retry
        if (retry instanceof RetryBackoffSpec) {
            return ((RetryBackoffSpec) retry)
                .withRetryContext(Context.of(Method.class, method));
        }
        return retry;
    }

    private void memberLeave(Member member) {
        String id = member.alias() == null ? member.id() : member.alias();
        ClusterNode ref = serverServiceRef.remove(id);
        if (null != ref) {
            fireEvent(ref.services.values(), id, ServiceEvent.Type.removed);
            ref.dispose();
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

    private void tryRelease(ServiceMessage serviceMessage) {
        if (ReferenceCountUtil.refCnt(serviceMessage.data()) > 0) {
            try {
                ReferenceCountUtil.release(serviceMessage.data());
            } catch (Throwable e) {
                log.warn("release service message data [{}] error", serviceMessage, e);
            }
        }
    }

    @SafeVarargs
    @SuppressWarnings("all")
    private static boolean hasException(Throwable e, Class<?>... target) {
        Throwable cause = e;
        while (cause != null) {
            for (Class<?> aClass : target) {
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


    class ClusterNode implements Disposable {
        private String id;

        private Member member;

        private Address rpcAddress;

        //private final Sinks.One<Void> close = Sinks.one();
        private final Map<String, Set<ServiceReferenceInfo>> serviceReferencesByQualifier = new NonBlockingHashMap<>();

        private final Map<String, ServiceRegistration> services = new NonBlockingHashMap<>();
        private final Map<Class<?>, ServiceInstances> serviceInstances = new NonBlockingHashMap<>();

        public boolean isSupported(RpcServiceCall<?> call) {
            return services.containsKey(call.namespace);
        }

        public synchronized void register(ServiceEndpoint endpoint) {
            List<String> readyToRemove = new ArrayList<>(serviceReferencesByQualifier.keySet());

            Map<String, ServiceRegistration> added = new HashMap<>();
            Map<String, ServiceRegistration> removed = new HashMap<>(services);

            log.debug("update service endpoint from [{}] : {} ", member, endpoint);

            for (ServiceRegistration registration : endpoint.serviceRegistrations()) {
                if (removed.remove(registration.namespace()) == null) {
                    added.put(registration.namespace(), registration);
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

            removed.forEach((k, v) -> services.remove(k));
            services.putAll(added);

            fireEvent(added.values(), id, ServiceEvent.Type.added);
            fireEvent(removed.values(), id, ServiceEvent.Type.removed);

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
                    ClusterNode node = ScalecubeRpcManager.this.serverServiceRef.get(this.id);
                    Set<ServiceReferenceInfo> refs = node == null ? null
                        : node.serviceReferencesByQualifier.get(request.qualifier());
                    if (refs == null) {
                        tryRelease(request);
                        return Optional.empty();
                    }
                    for (ServiceReferenceInfo ref : refs) {
                        return Optional.of(ref.reference);
                    }
                    tryRelease(request);
                    return Optional.empty();
                })
                .serviceRegistry(NoneServiceRegistry.INSTANCE)
                .errorMapper(errorMapper);

            return new RpcServiceCall<>(this.id,
                                        serviceId,
                                        name,
                                        api(call, serviceId, clazz),
                                        serviceId + "/" + name);
        }

        private <I> List<RpcService<I>> getApiCalls(Class<I> clazz) {
            return getApiCalls(null, clazz);
        }

        private String getServiceName(Class<?> clazz) {
            ServiceInstances instances = serviceInstances.get(clazz);
            if (instances != null) {
                return instances.name;
            }
            return Reflect.serviceName(clazz);
        }

        private <I, T> void getApiCalls(String id,
                                        Class<I> clazz,
                                        T container,
                                        BiConsumer<T, RpcService<I>> handler) {
            String sName = getServiceName(clazz);
            for (ServiceRegistration service : services.values()) {
                String name = service.tags().getOrDefault(SERVICE_NAME_TAG, service.namespace());
                String sid = service.tags().getOrDefault(SERVICE_ID_TAG, DEFAULT_SERVICE_ID);
                if (!Objects.equals(name, sName)) {
                    continue;
                }
                if (id != null && !Objects.equals(sid, id)) {
                    continue;
                }
                handler.accept(container, getApiCall(sid, clazz, service));
            }

        }

        private <I> List<RpcService<I>> getApiCalls(String id, Class<I> clazz) {
            List<RpcService<I>> container = new ArrayList<>(2);
            getApiCalls(id, clazz, container, List::add);
            return container;
        }

        @SuppressWarnings("all")
        private <I> RpcServiceCall<I> getApiCall(String id, Class<I> clazz, ServiceRegistration registration) {
            return (RpcServiceCall<I>) serviceInstances
                .computeIfAbsent(clazz, ServiceInstances::new)
                .computeIfAbsent(id, this::createApiCall);
        }

        @SuppressWarnings("all")
        private <I> RpcServiceCall<I> getApiCall(String id, Class<I> clazz) {
            return getApiCall(id, clazz, null);
        }

        private ServiceMessage.Builder toServiceMessageBuilder(MethodInfo methodInfo, Object request) {

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

            return builder;
        }

        private Mono<ServiceMessage> toServiceMessage(ContextView ctx,
                                                      MethodInfo methodInfo,
                                                      Object request,
                                                      SerializedContext serialized) {
            return Mono.fromSupplier(() -> toServiceMessage0(ctx, methodInfo, request, serialized));
        }

        private ServiceMessage toServiceMessage0(ContextView ctx,
                                                 MethodInfo methodInfo,
                                                 Object request,
                                                 SerializedContext serialized) {
            if (request instanceof ByteBuf buf) {
                // copy 新的 bytebuf,避免上游cancel时buf被提前release.
                request = buf.copy();
                ReferenceCountUtil.safeRelease(buf);
            }
            ServiceMessage.Builder builder = TraceHolder
                .writeContextTo(ctx, toServiceMessageBuilder(methodInfo, request), (ServiceMessage.Builder::header));

            String sctx = serialized.getContext();
            if (sctx != null) {
                builder = builder.header(HEADER_CONTEXT, sctx);
            }

            return TraceHolder
                .writeContextTo(ctx, builder, (ServiceMessage.Builder::header))
                .build();

        }


        private <T> Mono<T> wrap(ContextView ctx, Method method, Mono<T> origin) {
            if (ctx.hasKey(Retry.class)) {
                return origin;
            }
            return origin.retryWhen(getRetry(method));
        }

        private <T> Flux<T> wrap(ContextView ctx, Method method, Flux<T> origin) {
            if (ctx.hasKey(Retry.class)) {
                return origin;
            }
            return origin.retryWhen(getRetry(method));
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
                            Scheduler requestScheduler =
                                Schedulers.isInNonBlockingThread() ?
                                    Schedulers.immediate() : ScalecubeRpcManager.this.requestScheduler;

                            switch (methodInfo.communicationMode()) {
                                case FIRE_AND_FORGET:
                                    return Mono
                                        .deferContextual(ctx -> {
                                            SerializedContext serialize = contextCodec.serialize(ctx);
                                            return wrap(
                                                ctx,
                                                method,
                                                toServiceMessage(ctx, methodInfo, request, serialize)
                                                    .flatMap(msg -> serviceCall.oneWay(msg))
                                                    .doOnDiscard(
                                                        ServiceMessage.class,
                                                        ScalecubeRpcManager.this::tryRelease)
                                                    .subscribeOn(requestScheduler))
                                                .doFinally(ignore -> serialize.dispose());
                                        });

                                case REQUEST_RESPONSE:
                                    return Mono
                                        .deferContextual(ctx -> {
                                            SerializedContext serialize = contextCodec.serialize(ctx);
                                            return wrap(
                                                ctx,
                                                method,
                                                toServiceMessage(ctx, methodInfo, request, serialize)
                                                    .flatMap(msg -> serviceCall.requestOne(msg, returnType))
                                                    .doOnDiscard(
                                                        ServiceMessage.class,
                                                        ScalecubeRpcManager.this::tryRelease)
                                                    .subscribeOn(requestScheduler))
                                                .doFinally(ignore -> serialize.dispose());
                                        })
                                        .transform(asMono(isServiceMessage));

                                case REQUEST_STREAM:
                                    return Flux
                                        .deferContextual(ctx -> {
                                            SerializedContext serialize = contextCodec.serialize(ctx);
                                            return wrap(
                                                ctx,
                                                method,
                                                toServiceMessage(ctx, methodInfo, request, serialize)
                                                    .flatMapMany(msg -> serviceCall.requestMany(msg, returnType))
                                                    .doOnDiscard(
                                                        ServiceMessage.class,
                                                        ScalecubeRpcManager.this::tryRelease)
                                            )
                                                .doFinally(ignore -> serialize.dispose());
                                        })
                                        .transform(asFlux(isServiceMessage));

                                case REQUEST_CHANNEL:
                                    // this is REQUEST_CHANNEL so it means params[0] must
                                    // be a publisher - its safe to cast.
                                    //noinspection rawtypes
                                    return Flux
                                        .deferContextual(ctx -> {
                                            SerializedContext serialize = contextCodec.serialize(ctx);
                                            return wrap(ctx,
                                                        method,
                                                        serviceCall
                                                            .requestBidirectional(
                                                                Flux
                                                                    .from((Publisher<?>) request)
                                                                    .index((o, data) -> {
                                                                        if (o == 0) {
                                                                            return toServiceMessage0(ctx, methodInfo, data, serialize);
                                                                        }
                                                                        return toServiceMessageBuilder(methodInfo, data).build();
                                                                    }), returnType)
                                                            .subscribeOn(requestScheduler)
                                                            .doOnDiscard(
                                                                ServiceMessage.class,
                                                                ScalecubeRpcManager.this::tryRelease))
                                                .doFinally(ignore -> serialize.dispose());
                                        })
                                        .transform(asFlux(isServiceMessage));

                                default:
                                    throw new IllegalArgumentException(
                                        "Communication mode is not supported: " + method);
                            }
                        }
                    });
        }


        private Function<Flux<ServiceMessage>, Flux<Object>> asFlux(boolean isReturnTypeServiceMessage) {
            if (isReturnTypeServiceMessage) {
                return flux -> flux.cast(Object.class);
            }
            return flux -> flux.map(ServiceMessage::data);
        }

        private Function<Mono<ServiceMessage>, Mono<Object>> asMono(boolean isReturnTypeServiceMessage) {
            if (isReturnTypeServiceMessage) {
                return mono -> mono.cast(Object.class);
            }
            return mono -> mono.map(ServiceMessage::data);
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

        @Override
        public void dispose() {
//            close.emitError(
//                new ServiceUnavailableException("cluster node [" + member.alias() + "] is down"),
//                Reactors.emitFailureHandler());
        }
    }


    @SuppressWarnings("all")
    private static long hash(String server, Object key) {
        return HashUtils.murmur3_128(server, key);
    }


    static class ServiceInstances {
        private final Map<String, RpcServiceCall<?>> calls = new NonBlockingHashMap<>();

        @Getter
        private final String name;
        private final Class<?> type;

        ServiceInstances(Class<?> type) {
            this.type = type;
            this.name = Reflect.serviceName(type);
        }

        public RpcServiceCall<?> computeIfAbsent(String id,
                                                 BiFunction<String, Class<?>, RpcServiceCall<?>> supplier) {
            RpcServiceCall<?> fastPath = calls.get(id);
            if (fastPath != null) {
                return fastPath;
            }
            return calls.computeIfAbsent(id, (_id) -> supplier.apply(_id, type));
        }

        public RpcServiceCall<?> get(String callId) {
            return calls.get(callId);
        }

        private Collection<RpcServiceCall<?>> getAllCalls() {
            return calls.values();
        }
    }

    @AllArgsConstructor
    static class RpcServiceCall<T> implements RpcService<T> {
        private final String serverNodeId;
        private final String id;
        private final String name;
        private final T service;
        private final String namespace;

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

        @Override
        public String toString() {
            return name + "@" + serverNodeId;
        }
    }

    @EqualsAndHashCode(of = "id")
    @AllArgsConstructor
    static class ServiceReferenceInfo {
        private String id;
        private ServiceReference reference;
    }
}
