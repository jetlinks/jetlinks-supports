package org.jetlinks.supports.ipc;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.*;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class IpcRequestHandler<RES> implements Disposable {

    EmitterProcessor<RES> processor = EmitterProcessor.create(Integer.MAX_VALUE);

    Disposable.Composite disposable = Disposables.composite();

    FluxSink<RES> sink = processor.sink();

    private final AtomicInteger seq = new AtomicInteger();

    private final AtomicInteger totalSeq = new AtomicInteger(-1);

    IpcRequestHandler() {
    }

    Mono<RES> handleRequest() {
        return processor
                .next()
                .doAfterTerminate(()->{
                    disposable.dispose();
                });
    }

    Flux<RES> handleStream() {
        return processor
                .doAfterTerminate(()->{
                    disposable.dispose();
                });
    }

    void complete() {
        processor.onComplete();
        sink.complete();
    }

    void handle(IpcResponse<RES> res) {
        if (res.hasResult()) {
            sink.next(res.getResult());
        }
        if (res.getError() != null) {
            error(res.getError());
        } else {
            int resSeq = res.getSeq();
            int seq = resSeq < 0 ? 0 : this.seq.incrementAndGet();
            if (res.getType() == ResponseType.complete) {
                if (seq >= resSeq || totalSeq.get() != -1) {
                    complete();
                } else {
                    log.debug("ipc response complete early,seq[{}],total[{}]", this.seq, resSeq);
                    totalSeq.set(resSeq);
                }
            } else {
                if (totalSeq.get() > 0 && seq >= totalSeq.get()) {
                    sink.complete();
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
