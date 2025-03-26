package org.jetlinks.supports.collector;

import org.jetlinks.core.collector.DataCollectorProvider;
import org.jetlinks.core.collector.PointData;
import org.jetlinks.core.collector.PointProperties;
import org.jetlinks.core.collector.Result;
import org.jetlinks.core.monitor.Monitor;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import java.util.*;

public abstract class BatchPointCollectorRuntime<K> extends AbstractCollectorRuntime {

    protected BatchPointCollectorRuntime(String id,
                                         AbstractChannelRuntime channel,
                                         Monitor monitor) {
        super(id, channel, monitor);
    }

    @Override
    protected abstract void doStart();

    @Override
    public abstract AbstractPointRuntime createPoint(PointProperties properties);

    protected abstract Flux<Result<PointData>> doCollect(K groupKey,
                                                         List<DataCollectorProvider.PointRuntime> points);

    @Override
    protected Flux<Result<PointData>> doCollect(List<DataCollectorProvider.PointRuntime> points) {

        // 一个点位，直接读取。
        if (points.size() == 1) {
            return points.get(0).read().flux();
        }

        // 小于阈值，直接读。
        if (points.size() < getBatchThreshold()) {
            return Flux
                .fromIterable(points)
                .flatMap(DataCollectorProvider.PointRuntime::read, getCollectConcurrency());
        }

        // 分组，批量合并读。

        Map<K, List<DataCollectorProvider.PointRuntime>> group = new HashMap<>();
        int index = 0;

        for (DataCollectorProvider.PointRuntime point : points) {
            K key = batchKey(point, index++);

            group
                .computeIfAbsent(key, ignore -> new ArrayList<>())
                .add(point);
        }

        return Flux
            .fromIterable(group.entrySet())
            .flatMap(e -> doCollect(e.getKey(), e.getValue()), getCollectConcurrency());
    }

    protected int getCollectConcurrency() {
        return 1;
    }

    protected int getBatchThreshold() {
        return 2;
    }

    @Nonnull
    protected abstract K batchKey(DataCollectorProvider.PointRuntime point, int index);
}
