package org.jetlinks.supports.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetlinks.core.cluster.discovery.NodeIdentity;
import org.jetlinks.core.rpc.*;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.Collector;

/** 服务能力接口的本地实现。目录版本统一发布，连接和业务执行由 RpcManager 拥有。 */
public final class DefaultRpcServiceRegistry implements RpcServiceRegistry {
    record ServiceKey(String id, String name) {}
    record Endpoint(NodeIdentity node, ServiceKey service) {}
    record BoundMethod(RpcMethodBinding definition, MethodHandle handle) {
        Publisher<?> invoke(Object request) throws Throwable { return (Publisher<?>) handle.invokeExact(request); }
    }
    record Catalog(long revision, Set<ServiceKey> services) {}
    record LocalBindings(Map<ServiceKey, Map<String, BoundMethod>> services, Map<ByteBuf, BoundMethod> routes) {}
    record Directory(long revision, List<Endpoint> endpoints, Map<String, List<Endpoint>> byApi,
                     Map<String, Map<ServiceKey, Endpoint>> byNode, Set<NodeIdentity> unresolved) {}
    interface Invoker { Function<Object, Object> bind(String nodeId, ServiceKey key, RpcMethodBinding method, byte[] route); }

    private final NodeIdentity local;
    private final Invoker invoker;
    private final Runnable onLocalChange;
    private volatile LocalBindings localBindings = new LocalBindings(Map.of(), Map.of());
    private final Map<NodeIdentity, Set<ServiceKey>> peers = new HashMap<>();
    private final Map<String, AtomicInteger> cursors = new ConcurrentHashMap<>();
    private Set<NodeIdentity> members = Set.of();
    private final LatestValue<Directory> directory = new LatestValue<>(new Directory(0, List.of(), Map.of(), Map.of(), Set.of()));
    private volatile ClassValue<Map<Endpoint, RpcService<?>>> handles = handles();
    private boolean closed;
    private volatile Throwable failure;
    private long localRevision;

    DefaultRpcServiceRegistry(NodeIdentity local, Invoker invoker, Runnable onLocalChange) {
        this.local = local;
        this.invoker = invoker;
        this.onLocalChange = onLocalChange;
    }

    @Override public <T> Disposable registerService(T service) { return registerService("_default", service); }

    @Override public <T> Disposable registerService(String id, T service) {
        Map<ServiceKey, Map<String, BoundMethod>> added = new LinkedHashMap<>();
        for (Class<?> api : interfaces(service.getClass())) {
            var definition = RpcMethodBinding.APIS.get(api);
            if (definition.byName.isEmpty()) continue;
            Map<String, BoundMethod> methods = new HashMap<>();
            definition.byName.forEach((name, method) -> methods.put(name, new BoundMethod(method, method.bind(service))));
            added.put(new ServiceKey(id, definition.name), Map.copyOf(methods));
        }
        if (added.isEmpty()) throw new IllegalArgumentException("No reactive RPC interface: " + service.getClass());
        Map<ByteBuf, BoundMethod> addedRoutes = new HashMap<>();
        // Private read-only heap keys follow the immutable snapshot's GC lifetime, never a request buffer's lifetime.
        added.forEach((key, methods) -> methods.forEach((name, method) ->
            addedRoutes.put(Unpooled.wrappedBuffer(RpcProtocol.route(key, name)).asReadOnly(), method)));
        synchronized (this) {
            if (closed) throw new IllegalStateException("Registry closed");
            if (added.keySet().stream().anyMatch(localBindings.services::containsKey)) throw new IllegalArgumentException("RPC service already registered");
            Map<ServiceKey, Map<String, BoundMethod>> next = new HashMap<>(localBindings.services);
            next.putAll(added);
            Map<ByteBuf, BoundMethod> routes = new HashMap<>(localBindings.routes);
            routes.putAll(addedRoutes);
            localBindings = new LocalBindings(Map.copyOf(next), Map.copyOf(routes));
            localRevision++;
        }
        onLocalChange.run();
        return new Disposable() {
            private volatile boolean disposed;
            public void dispose() {
                synchronized (DefaultRpcServiceRegistry.this) {
                    if (disposed) return;
                    disposed = true;
                    Map<ServiceKey, Map<String, BoundMethod>> next = new HashMap<>(localBindings.services);
                    added.forEach((key, value) -> next.remove(key, value));
                    Map<ByteBuf, BoundMethod> routes = new HashMap<>(localBindings.routes);
                    addedRoutes.forEach((key, value) -> routes.remove(key, value));
                    localBindings = new LocalBindings(Map.copyOf(next), Map.copyOf(routes));
                    localRevision++;
                }
                onLocalChange.run();
            }
            public boolean isDisposed() { return disposed; }
        };
    }

    private static Set<Class<?>> interfaces(Class<?> type) {
        Set<Class<?>> result = new LinkedHashSet<>();
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            for (Class<?> api : current.getInterfaces()) collectInterfaces(api, result);
        }
        return result;
    }
    private static void collectInterfaces(Class<?> api, Set<Class<?>> result) {
        if (api.isAnnotationPresent(org.jetlinks.core.rpc.annotation.RpcApi.class)) result.add(api);
        else for (Class<?> parent : api.getInterfaces()) collectInterfaces(parent, result);
    }

    synchronized Catalog localCatalog() { return new Catalog(localRevision, Set.copyOf(localBindings.services.keySet())); }
    BoundMethod localMethod(ServiceKey key, String name) {
        Map<String, BoundMethod> service = localBindings.services.get(key);
        BoundMethod method = service == null ? null : service.get(name);
        if (method == null) throw new IllegalStateException("RPC service/method unavailable: " + key + "/" + name);
        return method;
    }
    BoundMethod localMethod(ByteBuf route) {
        BoundMethod method = localBindings.routes.get(route);
        if (method == null) throw new IllegalStateException("RPC service/method unavailable");
        return method;
    }
    synchronized void members(Set<NodeIdentity> values) {
        if (members.equals(values)) return;
        members = Set.copyOf(values);
        peers.keySet().retainAll(values);
        publish();
    }
    synchronized void ready(NodeIdentity node, Set<ServiceKey> catalog) {
        if (members.contains(node)) { peers.put(node, Set.copyOf(catalog)); publish(); }
    }
    synchronized void unavailable(NodeIdentity node) { if (peers.remove(node) != null) publish(); }
    synchronized void fail(Throwable error) { failure = error; directory.fail(error); }
    boolean isFailed() { return failure != null; }
    void checkAvailable() { if (failure != null) throw Exceptions.propagate(failure); }
    private Directory snapshot() { checkAvailable(); return directory.current(); }

    private void publish() {
        if (isFailed()) return;
        List<Endpoint> endpoints = new ArrayList<>();
        Map<String, List<Endpoint>> byApi = new HashMap<>();
        Map<String, Map<ServiceKey, Endpoint>> byNode = new HashMap<>();
        Set<String> duplicate = new HashSet<>();
        Set<String> identities = new HashSet<>();
        identities.add(local.nodeId());
        for (NodeIdentity node : members) {
            if (!identities.add(node.nodeId())) duplicate.add(node.nodeId());
        }
        peers.forEach((node, catalog) -> {
            if (!duplicate.contains(node.nodeId())) catalog.forEach(key -> endpoints.add(new Endpoint(node, key)));
        });
        endpoints.sort(Comparator.comparing((Endpoint e) -> e.node.nodeId()).thenComparing(e -> e.service.id).thenComparing(e -> e.service.name));
        endpoints.forEach(endpoint -> {
            byApi.computeIfAbsent(endpoint.service.name, ignored -> new ArrayList<>()).add(endpoint);
            byNode.computeIfAbsent(endpoint.node.nodeId(), ignored -> new HashMap<>()).put(endpoint.service, endpoint);
        });
        byApi.replaceAll((key, value) -> List.copyOf(value));
        byNode.replaceAll((key, value) -> Map.copyOf(value));
        cursors.keySet().retainAll(byApi.keySet());
        Set<NodeIdentity> unresolved = new HashSet<>(members);
        unresolved.removeAll(peers.keySet());
        handles = handles();
        directory.update(new Directory(directory.current().revision + 1, List.copyOf(endpoints), Map.copyOf(byApi), Map.copyOf(byNode), Set.copyOf(unresolved)));
    }

    private ClassValue<Map<Endpoint, RpcService<?>>> handles() {
        return new ClassValue<>() {
            protected Map<Endpoint, RpcService<?>> computeValue(Class<?> api) { return new ConcurrentHashMap<>(); }
        };
    }

    @SuppressWarnings("unchecked")
    private <I> RpcService<I> handle(Endpoint endpoint, Class<I> api) {
        return (RpcService<I>) handles.get(api).computeIfAbsent(endpoint, ignored -> {
            var definition = RpcMethodBinding.APIS.get(api);
            Map<Method, Function<Object, Object>> calls = new HashMap<>();
            definition.methods.forEach((method, binding) -> calls.put(method, invoker.bind(endpoint.node.nodeId(), endpoint.service, binding, RpcProtocol.route(endpoint.service, binding.name))));
            I proxy = (I) Proxy.newProxyInstance(api.getClassLoader(), new Class<?>[]{api}, (self, method, args) -> {
                if (method.getDeclaringClass() == Object.class) return switch (method.getName()) {
                    case "toString" -> "RPC[" + endpoint.node.nodeId() + "/" + endpoint.service + "]";
                    case "hashCode" -> System.identityHashCode(self);
                    case "equals" -> self == args[0];
                    default -> throw new IllegalStateException(method.toString());
                };
                return calls.get(method).apply(args == null ? null : args[0]);
            });
            return service(endpoint, proxy);
        });
    }

    private static <I> RpcService<I> service(Endpoint endpoint, I proxy) {
        return new RpcService<>() {
            public String serverNodeId() { return endpoint.node.nodeId(); }
            public String id() { return endpoint.service.id; }
            public String name() { return endpoint.service.name; }
            public I service() { return proxy; }
        };
    }

    private <I> List<Endpoint> endpoints(Class<I> api) {
        return snapshot().byApi.getOrDefault(RpcMethodBinding.APIS.get(api).name, List.of());
    }
    @Override public <I> Flux<RpcService<I>> getServices(Class<I> api) {
        return Flux.defer(() -> Flux.fromIterable(endpoints(api)).map(endpoint -> handle(endpoint, api)));
    }
    @Override public <I> Flux<RpcService<I>> getServices(String id, Class<I> api) {
        return getServices(api).filter(service -> service.id().equals(id));
    }
    @Override public Flux<RpcService<?>> getServices() {
        return Flux.defer(() -> Flux.fromIterable(snapshot().endpoints)
            .map(endpoint -> new RpcService<Object>() {
                public String serverNodeId() { return endpoint.node.nodeId(); }
                public String id() { return endpoint.service.id; }
                public String name() { return endpoint.service.name; }
                public Object service() { throw new UnsupportedOperationException("Use a typed service lookup to create an invocation handle"); }
            }));
    }
    @Override public <I> Mono<I> getService(String nodeId, Class<I> api) { return getService(nodeId, "_default", api); }
    @Override public <I> Mono<I> getService(String nodeId, String id, Class<I> api) {
        return Mono.fromSupplier(() -> {
            ServiceKey key = new ServiceKey(id, RpcMethodBinding.APIS.get(api).name);
            if (nodeId.equals(local.nodeId())) {
                if (!localBindings.services.containsKey(key)) return null;
                return handle(new Endpoint(local, key), api).service();
            }
            Endpoint found = snapshot().byNode.getOrDefault(nodeId, Map.of()).get(key);
            return found == null ? null : handle(found, api).service();
        });
    }

    @Override public <I> Mono<RpcService<I>> selectService(Class<I> api) { return selectService(api, (Object) null); }
    @Override public <I> Mono<RpcService<I>> selectService(Class<I> api, Object routeKey) {
        return Mono.fromSupplier(() -> {
            String name = RpcMethodBinding.APIS.get(api).name;
            List<Endpoint> values = snapshot().byApi.getOrDefault(name, List.of());
            if (values.isEmpty()) return null;
            Endpoint selected;
            if (routeKey == null) selected = values.get(Math.floorMod(cursors.computeIfAbsent(name, ignored -> new AtomicInteger()).getAndIncrement(), values.size()));
            else selected = values.stream().max(Comparator.comparingLong(endpoint -> hash(endpoint.node.nodeId(), routeKey))).orElseThrow();
            return handle(selected, api);
        });
    }
    private static long hash(String node, Object key) {
        long value = ((long) node.hashCode() << 32) ^ key.hashCode();
        value ^= value >>> 33; value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33; value *= 0xc4ceb9fe1a85ec53L;
        return value ^ value >>> 33;
    }
    @Override public <T, I> Mono<I> selectService(Class<I> api,
            Collector<RpcService<I>, T, Optional<RpcService<I>>> selector, Mono<I> fallback) {
        var definition = RpcMethodBinding.APIS.get(api);
        I proxy = api.cast(Proxy.newProxyInstance(api.getClassLoader(), new Class<?>[]{api}, (self, method, args) -> {
            if (method.getDeclaringClass() == Object.class) return switch (method.getName()) {
                case "toString" -> "RPC selection[" + definition.name + "]";
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == args[0];
                default -> throw new IllegalStateException(method.toString());
            };
            RpcMethodBinding binding = definition.methods.get(method);
            Object request = args == null || args.length == 0 ? null : args[0];
            Supplier<Optional<RpcService<I>>> select = () -> {
                T state = selector.supplier().get();
                for (Endpoint endpoint : endpoints(api)) selector.accumulator().accept(state, handle(endpoint, api));
                return selector.finisher().apply(state);
            };
            if (binding.stream) return Flux.defer(() -> {
                Optional<RpcService<I>> selected = select.get();
                if (selected.isPresent()) return Flux.from(binding.invokeClient(selected.get().service(), request));
                return fallback.flatMapMany(service -> binding.invokeClient(service, request));
            });
            return Mono.defer(() -> {
                Optional<RpcService<I>> selected = select.get();
                if (selected.isPresent()) return Mono.from(binding.invokeClient(selected.get().service(), request));
                return fallback.flatMap(service -> Mono.from(binding.invokeClient(service, request)));
            });
        }));
        return Mono.just(proxy);
    }
    @Override public <I> Flux<RpcServiceView<I>> watchServices(Class<I> api) {
        return directory.watch().map(snapshot -> new RpcServiceView<>(snapshot.revision,
            snapshot.byApi.getOrDefault(RpcMethodBinding.APIS.get(api).name, List.of()).stream().map(endpoint -> handle(endpoint, api)).toList(), snapshot.unresolved));
    }
    @Override public <I> Flux<ServiceEvent> listen(Class<I> api) {
        String name = RpcMethodBinding.APIS.get(api).name;
        return directory.watch().buffer(2, 1).filter(pair -> pair.size() == 2).concatMapIterable(pair -> {
            Set<Endpoint> before = new LinkedHashSet<>(pair.get(0).byApi.getOrDefault(name, List.of()));
            Set<Endpoint> after = new LinkedHashSet<>(pair.get(1).byApi.getOrDefault(name, List.of()));
            List<ServiceEvent> events = new ArrayList<>();
            before.stream().filter(endpoint -> !after.contains(endpoint)).forEach(endpoint -> events.add(event(endpoint, ServiceEvent.Type.removed)));
            after.stream().filter(endpoint -> !before.contains(endpoint)).forEach(endpoint -> events.add(event(endpoint, ServiceEvent.Type.added)));
            return events;
        });
    }
    private static ServiceEvent event(Endpoint endpoint, ServiceEvent.Type type) {
        return new ServiceEvent(endpoint.service.id, endpoint.service.name, endpoint.node.nodeId(), type);
    }
    synchronized void close() {
        closed = true;
        localBindings = new LocalBindings(Map.of(), Map.of()); peers.clear(); members = Set.of();
        cursors.clear();
        if (isFailed()) handles = handles();
        publish(); directory.close();
    }
}
