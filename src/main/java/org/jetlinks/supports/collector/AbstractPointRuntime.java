package org.jetlinks.supports.collector;

import org.hswebframework.web.dict.EnumDict;
import org.jetlinks.core.collector.*;
import org.jetlinks.core.monitor.Monitor;
import reactor.core.publisher.Mono;

public abstract class AbstractPointRuntime extends AbstractLifecycle implements DataCollectorProvider.PointRuntime {

    private final String id;

    private final long accessMod;

    private final Monitor monitor;

    public AbstractPointRuntime(Monitor monitor, PointProperties properties) {
        this.id = properties.getId();
        this.monitor = monitor;
        properties.copyTo(this);
        if (properties.getAccessModes() != null) {
            this.accessMod = EnumDict.toMask(properties.getAccessModes());
        } else {
            this.accessMod = 0;
        }
    }

    @Override
    public final String getId() {
        return id;
    }

    protected abstract Mono<PointData> doRead();

    protected abstract Mono<PointData> doWrite(PointData data);

    protected abstract Mono<Result<Void>> doTest();

    @Override
    public final Mono<Result<Void>> test() {
        return doTest();
    }

    protected boolean hasAccessMode(AccessMode mode) {
        return EnumDict.maskIn(accessMod, mode);
    }

    @Override
    public final Mono<Result<PointData>> read() {
        if (hasAccessMode(AccessMode.read)) {
            return this
                .doRead()
                .map(Result::success)
                .as(monitor.tracer().traceMono("read"))
                .onErrorResume(err -> Mono.just(Result.error(err)));
        }
        return Mono.just(Result.error(CollectorConstants.Codes.pointUnsupportedRead.getCode()));
    }

    @Override
    public final Mono<Result<PointData>> write(PointData data) {
        if (hasAccessMode(AccessMode.write)) {
            return this
                .doWrite(data)
                .map(Result::success)
                .as(monitor.tracer().traceMono("write"))
                .onErrorResume(err -> Mono.just(Result.error(err)));
        }
        return Mono.just(Result.error(CollectorConstants.Codes.pointUnsupportedWrite.getCode()));
    }

    protected abstract Mono<Result<Void>> validate();

    @Override
    protected void doStart() {

    }
}
