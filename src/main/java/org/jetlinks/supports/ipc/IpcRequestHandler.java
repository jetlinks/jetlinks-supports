package org.jetlinks.supports.ipc;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class IpcRequestHandler<RES> implements Disposable {

    Sinks.Many<RES> processorSinksMany = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE);

    Disposable.Composite disposable = Disposables.composite();

    private final AtomicInteger seqInc = new AtomicInteger();

    private final AtomicInteger totalSeq = new AtomicInteger(-1);

    IpcRequestHandler() {
    }

    Mono<RES> handleRequest() {
        return processorSinksMany
                .asFlux()
                .next()
                .doFinally((s) -> disposable.dispose());
    }

    Flux<RES> handleStream() {
        return processorSinksMany
                .asFlux()
                .doFinally((s) -> disposable.dispose());
    }

    void complete() {
        processorSinksMany.tryEmitComplete();
    }

    synchronized void handle(IpcResponse<RES> res) {
        if (res.hasResult()) {
            processorSinksMany.tryEmitNext(res.getResult());
        }
        if (res.hasError()) {
            error(res.getError());
        } else {
            int resSeq = res.getSeq();
            int seq = resSeq < 0 ? -1 : this.seqInc.getAndIncrement();
            if (res.getType() == ResponseType.complete) {
                if (resSeq < 0 || seq > resSeq || totalSeq.get() != -1) {
                    complete();
                } else {
                    log.debug("ipc response complete early,seq[{}],total[{}]", this.seqInc, resSeq);
                    totalSeq.set(resSeq);
                }
            } else {
                int total = totalSeq.get();
                if (total >= 0 && seq + 1 >= total) {
                    complete();
                }
            }
        }
    }

    void error(Throwable err) {
        processorSinksMany.tryEmitError(err);
    }

    IpcRequestHandler<RES> doOnDispose(Disposable disposable) {
        this.disposable.add(disposable);
        return this;
    }

    @Override
    public void dispose() {
        complete();
    }
}
