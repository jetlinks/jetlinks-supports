package org.jetlinks.supports.collector;

import com.google.common.collect.Collections2;
import org.jetlinks.core.collector.DataCollectorProvider;
import org.jetlinks.core.collector.PointData;
import org.jetlinks.core.collector.PointProperties;
import org.jetlinks.core.collector.Result;
import org.jetlinks.core.monitor.Monitor;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public abstract class AbstractCollectorRuntime extends AbstractLifecycle implements DataCollectorProvider.CollectorRuntime {

    private final String id;

    protected final Map<String, AbstractPointRuntime> points = new ConcurrentHashMap<>();

    private final List<Consumer<PointData>> listeners = new CopyOnWriteArrayList<>();

    protected final AbstractChannelRuntime channel;

    protected Monitor monitor;

    protected AbstractCollectorRuntime(String id,
                                       AbstractChannelRuntime channel,
                                       Monitor monitor) {
        this.id = id;
        this.monitor = monitor;
        this.channel = channel;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    protected abstract void doStart();

    public abstract AbstractPointRuntime createPoint(PointProperties properties);

    protected abstract Flux<Result<PointData>> doCollect(List<DataCollectorProvider.PointRuntime> points);

    @Override
    public final Mono<Void> registerPoint(Collection<PointProperties> points) {
        return Flux
            .fromIterable(points)
            .concatMap(this::registerPoint)
            .then();
    }

    private Mono<Void> registerPoint(PointProperties properties) {
        AbstractPointRuntime runtime = createPoint(properties);
        AbstractPointRuntime old = points.put(runtime.getId(), runtime);
        if (old != null) {
            return old
                .shutdown()
                .then(runtime.start())
                .as(monitor.tracer().traceMono("register"));
        }
        return runtime
            .start()
            .as(monitor.tracer().traceMono("register"));
    }

    @Override
    public Mono<Result<Void>> validatePoint(PointProperties properties) {
        return createPoint(properties).validate();
    }

    @Override
    public Mono<Void> unregisterPoint(Collection<String> idList) {
        return Flux
            .fromIterable(idList)
            .mapNotNull(points::remove)
            .flatMap(DataCollectorProvider.PointRuntime::shutdown)
            .then();
    }

    @Override
    public final Mono<DataCollectorProvider.PointRuntime> getPoint(String id) {
        return Mono.fromSupplier(() -> points.get(id));
    }

    @Override
    public final Flux<DataCollectorProvider.PointRuntime> getPoints(Collection<String> idList) {
        return Flux
            .fromIterable(idList)
            .mapNotNull(points::get);
    }

    @Override
    public final Flux<DataCollectorProvider.PointRuntime> getPoints() {
        return Flux.fromIterable(points.values());
    }

    @Override
    public final Disposable subscribe(Consumer<PointData> listener) {
        listeners.add(listener);

        return () -> listeners.remove(listener);
    }

    @Override
    public final Flux<Result<PointData>> collect(Collection<String> idList) {

        return doCollect(new ArrayList<>(
            Collections2.filter(
                Collections2.transform(idList, points::get),
                Objects::nonNull
            )
        ));
    }


    protected void handleSubscribedPoint(PointData data) {

        monitor
            .logger()
            .trace("message.collector.point.subscribed", data);

        for (Consumer<PointData> listener : listeners) {
            listener.accept(data);
        }
    }
}
