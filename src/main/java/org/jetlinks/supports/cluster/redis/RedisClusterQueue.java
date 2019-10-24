package org.jetlinks.supports.cluster.redis;

import org.jetlinks.core.cluster.ClusterQueue;
import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("all")
public class RedisClusterQueue<T> implements ClusterQueue<T> {

    private final String id;

    private ReactiveRedisOperations<String, T> operations;

    private FluxProcessor<T, T> processor = EmitterProcessor.create(512);

    private AtomicBoolean polling = new AtomicBoolean(false);

    private volatile Disposable disposable;

    private volatile Disposable timer;

    private int batchSize = 32;

    private volatile float localConsumerPercent = 1F;

    @Override
    public void setLocalConsumerPercent(float localConsumerPercent) {
        this.localConsumerPercent = localConsumerPercent;
    }

    private final RedisScript<List<T>> batchPollScript = RedisScript.of(
            "local val = redis.call('lrange',KEYS[1],0," + batchSize + ");" +
                    "redis.call('ltrim',KEYS[1]," + (batchSize + 1) + ",-1);" +
                    "return val;"
            , List.class
    );

    private final RedisScript<Long> pushAndPublish;

    public RedisClusterQueue(String id, ReactiveRedisOperations<String, T> operations) {
        this.id = id;
        this.operations = operations;
        pushAndPublish = RedisScript.of(
                "local val = redis.call('lpush',KEYS[1],ARGV[1]);" +
                        "redis.call('publish'," + "'queue:data:produced:".concat(id) + "',ARGV[2]);" +
                        "return val;"
                , Long.class
        );
    }

    protected void startPoll() {
        disposable = operations
                .listenToChannel("queue:data:produced:".concat(id))
                .map(ReactiveSubscription.Message::getMessage)
                .subscribe(sub -> doPoll());

        timer = Flux.interval(Duration.ofSeconds(5))
                .subscribe(r -> doPoll());
    }

    protected void doPoll() {
        if (polling.compareAndSet(false, true)) {
            if (!processor.hasDownstreams()) {
                stopPoll();
                return;
            }
            pollBatch()
                    .doOnNext(processor::onNext)
                    .count()
                    .doFinally((s) -> polling.set(false))
                    .subscribe(r -> {
                        if (r >= batchSize) { //继续poll
                            polling.set(false);
                            doPoll();
                        }
                    });
        }
    }

    protected void stopPoll() {
        disposable.dispose();
        timer.dispose();
    }

    @Override
    public Flux<T> subscribe() {
        return processor
                .doOnSubscribe(sub -> startPoll())
                .doFinally(s -> stopPoll());
    }

    @Override
    public Mono<T> poll() {
        return operations
                .opsForList()
                .leftPop(id);
    }

    private Flux<T> pollBatch() {
        return this.operations
                .execute(batchPollScript, Collections.singletonList(id))
                .flatMap(Flux::fromIterable);
    }

    private ReactiveRedisOperations getOperations() {
        return operations;
    }

    @Override
    public Mono<Boolean> add(Publisher<T> publisher) {
        return Flux.from(publisher)
                .flatMap(v -> {
                    if (processor.hasDownstreams() && Math.random() < localConsumerPercent) {
                        processor.onNext(v);
                        return Mono.just(1);
                    } else {
                        return getOperations().execute(pushAndPublish, Arrays.asList(id), Arrays.asList(v, "1"));
                    }
                })
                .then(Mono.just(true));
    }

    @Override
    public Mono<Boolean> addBatch(Publisher<? extends Collection<T>> publisher) {
        return Flux.from(publisher)
                .flatMap(v -> {
                            if (processor.hasDownstreams() && Math.random() < localConsumerPercent) {
                                v.forEach(processor::onNext);
                                return Mono.just(1);
                            }
                            return this.operations
                                    .opsForList()
                                    .leftPushAll(id, v)
                                    .doOnNext(l -> getOperations()
                                            .convertAndSend("queue:data:produced:".concat(id), "1"));
                        }
                )
                .then(Mono.just(true));
    }
}
