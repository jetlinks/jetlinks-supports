package org.jetlinks.supports.collector;

import com.google.common.collect.Collections2;
import org.jetlinks.core.collector.CollectorProperties;
import org.jetlinks.core.collector.DataCollectorProvider;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractChannelRuntime extends AbstractLifecycle implements DataCollectorProvider.ChannelRuntime {

    private final Map<String, DataCollectorProvider.CollectorRuntime> collectors =
        new ConcurrentHashMap<>();

    private final String id;

    private DataCollectorProvider.ChannelConfiguration configuration;

    public AbstractChannelRuntime(String id,
                                  DataCollectorProvider.ChannelConfiguration configuration) {
        this.id = id;
        this.configuration = configuration;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public final Mono<Void> registerCollector(Collection<CollectorProperties> properties) {

        return Flux
            .fromIterable(properties)
            .flatMap(this::registerCollector)
            .then();
    }

    private Mono<Void> registerCollector(CollectorProperties properties) {
        DataCollectorProvider
            .CollectorRuntime runtime = createCollector(properties);

        DataCollectorProvider
            .CollectorRuntime old = collectors.put(runtime.getId(), runtime);

        if (old != null) {
            return old
                .shutdown()
                .then(runtime.start());
        }

        return runtime.start();

    }

    protected abstract DataCollectorProvider.CollectorRuntime createCollector(CollectorProperties properties);

    protected abstract DataCollectorProvider.CollectorRuntime reloadCollector(
        CollectorProperties properties,
        DataCollectorProvider.CollectorRuntime old);

    @Override
    public final Mono<Void> unregisterCollector(Collection<String> idList) {

        return Flux
            .fromIterable(idList)
            .flatMap(id -> {
                DataCollectorProvider.CollectorRuntime runtime = collectors.remove(id);
                if (null != runtime) {
                    return runtime.shutdown();
                }
                return Mono.empty();
            })
            .then();
    }

    @Override
    public final Mono<DataCollectorProvider.CollectorRuntime> getCollector(String id) {
        return Mono.fromSupplier(() -> collectors.get(id));
    }

    @Override
    public final Flux<DataCollectorProvider.CollectorRuntime> getCollectors(Collection<String> id) {
        return Flux.fromIterable(
            Collections2.filter(Collections2.transform(id, collectors::get), Objects::nonNull)
        );
    }

}
