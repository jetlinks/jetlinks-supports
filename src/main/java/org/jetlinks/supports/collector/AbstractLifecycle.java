package org.jetlinks.supports.collector;

import org.jetlinks.core.collector.CollectorConstants;
import org.jetlinks.core.collector.DataCollectorProvider;
import org.jetlinks.core.command.AbstractCommandSupport;
import org.jetlinks.core.command.CommandSupport;
import org.jetlinks.core.command.ProxyCommandSupport;
import org.jetlinks.core.command.ProxyCommandSupportAdapter;
import org.jetlinks.supports.command.JavaBeanCommandSupport;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class AbstractLifecycle extends JavaBeanCommandSupport implements DataCollectorProvider.Lifecycle {

    private static final AtomicReferenceFieldUpdater<AbstractLifecycle, DataCollectorProvider.State>
        STATE = AtomicReferenceFieldUpdater.newUpdater(AbstractLifecycle.class, DataCollectorProvider.State.class, "state");


    private volatile DataCollectorProvider.State state = CollectorConstants.States.running;
    private final Disposable.Composite disposable = Disposables.composite();

    public AbstractLifecycle() {
        super();
    }

    protected boolean changeState(DataCollectorProvider.State state) {
        DataCollectorProvider.State old = STATE.getAndSet(this, state);

        if (!Objects.equals(old, state)) {
            onStateChanged(old, state);
            return true;
        }

        return false;
    }

    protected void onStateChanged(DataCollectorProvider.State before,
                                  DataCollectorProvider.State after) {

    }

    @Override
    public final Mono<DataCollectorProvider.State> state() {
        return Mono.just(state);
    }

    protected abstract void doStart();

    protected Mono<DataCollectorProvider.State> doCheckState() {
        return Mono.empty();
    }

    @Override
    public final Mono<DataCollectorProvider.State> checkState() {
        return this
            .doCheckState()
            .doOnNext(this::changeState)
            .thenReturn(state);
    }

    @Override
    public final Mono<Void> start() {
        return Mono.fromRunnable(() -> {
            try {
                if (changeState(CollectorConstants.States.starting)) {
                    doStart();
                }
            } catch (Throwable e) {
                changeState(CollectorConstants.States.stopped);
                throw e;
            }
        });
    }

    protected void doOnShutdown(Disposable disposable) {
        this.disposable.add(disposable);
    }

    @Override
    public final Mono<Void> shutdown() {
        return Mono.fromRunnable(() -> {
            disposable.dispose();
            changeState(CollectorConstants.States.shutdown);
        });
    }
}
