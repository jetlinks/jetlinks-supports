package org.jetlinks.supports.ipc;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class IpcRequestHandler<RES> implements Disposable {

    EmitterProcessor<RES> processor = EmitterProcessor.create(Integer.MAX_VALUE);

    Disposable.Composite disposable = Disposables.composite();

    FluxSink<RES> sink = processor.sink();

    private final AtomicInteger seqInc = new AtomicInteger();

    private final AtomicInteger totalSeq = new AtomicInteger(-1);

    IpcRequestHandler() {
    }

    Mono<RES> handleRequest() {
        return processor
                .next()
                .doFinally((s) -> disposable.dispose());
    }

    Flux<RES> handleStream() {
        return processor
                .doFinally((s) -> disposable.dispose());
    }

    void complete() {
        if(processor.isDisposed()){
            log.debug("handler is disposed");
        }
        processor.onComplete();
        sink.complete();
    }

    synchronized void handle(IpcResponse<RES> res) {
        if (res.hasResult()) {
            sink.next(res.getResult());
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
        sink.error(err);
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
