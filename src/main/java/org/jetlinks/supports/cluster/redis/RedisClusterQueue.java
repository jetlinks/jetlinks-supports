package org.jetlinks.supports.cluster.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterQueue;
import org.reactivestreams.Publisher;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("all")
@Slf4j
public class RedisClusterQueue<T> implements ClusterQueue<T> {

    private final String id;

    protected final ReactiveRedisOperations<String, T> operations;

    private AtomicBoolean polling = new AtomicBoolean(false);

    private int maxBatchSize = 32;

    private volatile float localConsumerPercent = 1F;

    private long lastRequestSize = maxBatchSize;

    private Mod mod = Mod.FIFO;

    private List<FluxSink<T>> subscribers = new CopyOnWriteArrayList<>();

    @Override
    public void setLocalConsumerPercent(float localConsumerPercent) {
        this.localConsumerPercent = localConsumerPercent;
    }

    private static final RedisScript<List<?>> lifoPollScript = RedisScript.of(
            String.join("\n"
                    , "local val = redis.call('lrange',KEYS[1],0,KEYS[2]);"
                    , "redis.call('ltrim',KEYS[1],KEYS[2]+1,-1);"
                    , "return val;")
            , List.class
    );

    private static final RedisScript<List<?>> fifoPollScript = RedisScript.of(
            String.join("\n"
                    , "local size = redis.call('llen',KEYS[1]);"
                    , "if size == 0 then"
                    , "return nil"
                    , "end"
                    , "local index = size - KEYS[2];"
                    , "if index == 0 then"
                    , "return redis.call('lpop',KEYS[1]);"
                    , "end"
                    , "local val = redis.call('lrange',KEYS[1],index,size);"
                    , "redis.call('ltrim',KEYS[1],0,index-1);"
                    , "return val;")
            , List.class
    );

    private static final RedisScript<Long> pushAndPublish = RedisScript.of(
            "local val = redis.call('lpush',KEYS[1],ARGV[1]);" +
                    "redis.call('publish','queue:data:produced',ARGV[2]);" +
                    "return val;"
            , Long.class
    );

    public RedisClusterQueue(String id, ReactiveRedisTemplate<String, T> operations) {
        this.id = id;
        this.operations = operations;
    }

    protected void tryPoll() {
        doPoll(lastRequestSize);
    }

    AtomicInteger lastPush = new AtomicInteger(0);

    private boolean push(Iterable<T> data) {
        for (T datum : data) {
            if (!push(datum)) {
                return false;
            }
        }
        return true;
    }

    private boolean push(T data) {
        int size = subscribers.size();
        if (size == 0) {
            return false;
        }
        if (size == 1) {
            subscribers.get(0).next(data);
            return true;
        }
        if (lastPush.incrementAndGet() >= size) {
            lastPush.set(0);
        }
        subscribers.get(lastPush.get()).next(data);
        return true;
    }

    private void doPoll(long size) {
        if (subscribers.size() <= 0) {
            return;
        }
        if (polling.compareAndSet(false, true)) {

            AtomicLong total = new AtomicLong(size);
            long pollSize = Math.min(total.get(), maxBatchSize);

            log.trace("poll datas[{}] from redis [] ", pollSize, id);
            pollBatch((int) pollSize)
                    .flatMap(v -> {
                        //没有订阅者了,重入队列
                        if (!push(v)) {
                            return operations
                                    .opsForList()
                                    .leftPush(id, v)
                                    .then();
                        } else {
                            return Mono.just(v);
                        }
                    })
                    .count()
                    .doFinally((s) -> polling.set(false))
                    .subscribe(r -> {
                        if (r > 0 && total.addAndGet(-r) > 0) { //继续poll
                            polling.set(false);
                            doPoll(total.get());
                        }
                    });
        }
    }

    protected void stopPoll() {

    }

    @Nonnull
    @Override
    public Flux<T> subscribe() {
        return Flux
                .<T>create(sink -> {
                    subscribers.add(sink);
                    sink.onDispose(() -> {
                        subscribers.remove(sink);
                    });
                    doPoll(sink.requestedFromDownstream());
                })
                .doOnRequest(i -> {
                    doPoll(lastRequestSize = i);
                });
    }

    @Override
    public void stop() {
        stopPoll();
    }

    @Override
    public Mono<Integer> size() {
        return operations.opsForList()
                .size(id)
                .map(Number::intValue);
    }

    @Override
    public void setPollMod(Mod mod) {
        this.mod = mod;
    }

    @Nonnull
    @Override
    public Mono<T> poll() {
        return mod == Mod.LIFO
                ? operations.opsForList().leftPop(id)
                : operations.opsForList().rightPop(id);
    }

    private Flux<T> pollBatch(int size) {
        if (size == 1) {
            return poll()
                    .flux();
        }
        return
                (
                        mod == Mod.FIFO
                                ? this
                                .operations
                                .execute(fifoPollScript, Arrays.asList(id, String.valueOf(size)))
                                .doOnNext(list -> {
                                    Collections.reverse(list); //先进先出,反转顺序
                                })
                                : this.operations.execute(lifoPollScript, Arrays.asList(id, String.valueOf(size)))
                )
                        .flatMap(Flux::fromIterable)
                        .map(i -> (T) i);

    }

    private ReactiveRedisOperations getOperations() {
        return operations;
    }

    private boolean isLocalConsumer() {
        return subscribers.size() > 0 && (localConsumerPercent == 1F || ThreadLocalRandom.current().nextFloat() < localConsumerPercent);
    }

    @Override
    public Mono<Boolean> add(Publisher<T> publisher) {
        return Flux
                .from(publisher)
                .flatMap(v -> {
                    if (isLocalConsumer() && push(v)) {
                        return Mono.just(1);
                    } else {
                        return getOperations().execute(pushAndPublish, Arrays.asList(id), Arrays.asList(v, id));
                    }
                })
                .then(Mono.just(true));
    }

    @Override
    public Mono<Boolean> addBatch(Publisher<? extends Collection<T>> publisher) {
        return Flux.from(publisher)
                .flatMap(v -> {
                            if (isLocalConsumer() && push(v)) {
                                return Mono.just(1);
                            }
                            return this.operations
                                    .opsForList()
                                    .leftPushAll(id, v)
                                    .then(getOperations().convertAndSend("queue:data:produced", id));
                        }
                )
                .then(Mono.just(true));
    }
}
