package org.jetlinks.supports.cache;

import lombok.SneakyThrows;
import org.jetlinks.core.cache.FileQueue;
import org.jetlinks.core.cache.FileQueueProxy;
import org.jetlinks.core.utils.Reactors;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Subscription;
import org.springframework.util.unit.DataSize;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MVStoreQueueBuilderFactoryTest {


    @Test
    public void testAutoReload() {
        MVStoreQueue<Integer> queue = new MVStoreQueue<>(
            Paths.get("./target/testAutoReload.queue"),
            "test",
            Collections.emptyMap()
        );
        new File("./target/testAutoReload.queue/test").deleteOnExit();

        int count = 10_0000;

        Flux.range(1, count)
            .doOnNext(i -> {
                if (i % 10000 == 0) {
                    queue.store.close(1000);
                }
            })
            .flatMap(i -> Mono.fromRunnable(() -> queue.offer(i))
                              .subscribeOn(Schedulers.parallel()))
            .then(Mono.fromSupplier(queue::size))
            .as(StepVerifier::create)
            .expectNext(count)
            .verifyComplete();

    }

    @Test
    @SneakyThrows
    public void testBack() {
        AtomicInteger total = new AtomicInteger(100_0000);
        int size = total.get();
        Sinks.Many<Integer> sink = FileQueue
            .<Integer>builder()
            .name("test")
            .path(Paths.get("./target/buf.queue"))
            .buildFluxProcessor(false);

        sink.asFlux()
            .subscribe(new BaseSubscriber<Integer>() {
                final AtomicInteger count = new AtomicInteger();

                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    subscription.request(1000);
                }

                @Override
                @SneakyThrows
                protected void hookOnNext(Integer value) {
                    total.decrementAndGet();
                    if (count.incrementAndGet() >= 1000) {
                        count.set(0);
                        request(1000);
                        Thread.sleep(1);
                    }
                }
            });

        Duration time = Flux.range(0, size)
                            .flatMap(i -> Mono
                                .fromRunnable(() -> sink.emitNext(i, Reactors.emitFailureHandler()))
                                .subscribeOn(Schedulers.parallel()))
                            .then()
                            .as(StepVerifier::create)
                            .expectComplete()
                            .verify();
        System.out.println(time);
        sink.tryEmitComplete();
        assertEquals(0, total.get());

    }


    @Test
    @Ignore
    public void benchmark() {
        FileQueue<byte[]> queue = FileQueue
            .<byte[]>builder()
            .name("benchmark")
            .path(Paths.get("./target/benchmark-queue"))
            .option("concurrency", 4)
            .build();
        new File("./target/benchmark-queue/benchmark").deleteOnExit();


        int size = 100_0000;

        Duration time = Flux
            .range(0, size)
            .flatMap(i -> Mono
                .fromRunnable(() -> {
                    byte[] bytes = new byte[1024];
                    ThreadLocalRandom.current().nextBytes(bytes);
                    queue.offer(bytes);
                }))
            .then()
            .as(StepVerifier::create)
            .expectComplete()
            .verify();
        System.out.println(time);

        System.out.println(queue.size());
        // assertEquals(size, queue.size());
        queue.close();
        System.out.println(
            DataSize
                .ofBytes(new File("./target/benchmark-queue/benchmark").length())
                .toMegabytes());

    }

    @Test
    @SneakyThrows
    public void test() {

        FileQueue<String> strings = FileQueue.<String>builder()
                                             .name("test")
                                             .path(Paths.get("./target/.queue"))
                                             .build();
        int numberOf = 20_0000;
        long time = System.currentTimeMillis();
        Duration writeTime = Flux
            .range(0, numberOf)
            .doOnNext(i -> {
                strings.add("data:" + i);
            })
            .buffer(10000)
            .then()
            .as(StepVerifier::create)
            .verifyComplete();
        System.out.println("writeTime:" + writeTime);
        strings.flush();
        assertEquals(strings.size(), numberOf);

        Flux.fromIterable(strings)
            .distinct()
            .as(StepVerifier::create)
            .expectNextCount(numberOf)
            .verifyComplete();

        Duration pollTime = Flux
            .range(0, numberOf)
            .map(i -> strings.poll())
            .as(StepVerifier::create)
            .expectNextCount(numberOf)
            .verifyComplete();
        System.out.println("pollTime:" + pollTime);

        assertTrue(strings.isEmpty());

        System.out.println(System.currentTimeMillis() - time);
        strings.flush();
        strings.close();
    }

    @Test
    public void testFlux() {
        Sinks.Many<String> processor = FileQueue
            .<String>builder()
            .name("test-flux")
            .path(Paths.get("./target/.queue"))
            .buildFluxProcessor(true);

        processor.tryEmitNext("test");
        processor
            .asFlux()
            .take(1)
            .as(StepVerifier::create)
            .expectNext("test")
            .verifyComplete();

    }

    @Test
    public void pollFillsCallerCollectionInFifoOrder() {
        MVStoreQueue<Integer> queue = new MVStoreQueue<>(
            Paths.get("./target/poll-fifo.queue"),
            "test",
            Collections.emptyMap()
        );
        new File("./target/poll-fifo.queue/test").deleteOnExit();
        try {
            for (int i = 0; i < 1000; i++) {
                queue.add(i);
            }
            List<Integer> batch = new ArrayList<>();
            assertEquals(64, queue.poll(64, batch));
            assertEquals(64, batch.size());
            for (int i = 0; i < 64; i++) {
                assertEquals(Integer.valueOf(i), batch.get(i));
            }
            assertEquals(936, queue.size());
            assertEquals(0, queue.poll(0, batch));
            assertEquals(0, queue.poll(-1, batch));
            assertEquals(64, batch.size());
        } finally {
            queue.close();
        }
    }

    @Test
    public void pollLastFillsCallerCollectionNewestFirst() {
        MVStoreQueue<Integer> queue = new MVStoreQueue<>(
            Paths.get("./target/poll-last.queue"),
            "test",
            Collections.emptyMap()
        );
        new File("./target/poll-last.queue/test").deleteOnExit();
        try {
            for (int i = 0; i < 1000; i++) {
                queue.add(i);
            }
            List<Integer> batch = new ArrayList<>();
            assertEquals(64, queue.pollLast(64, batch));
            assertEquals(64, batch.size());
            for (int i = 0; i < 64; i++) {
                assertEquals(Integer.valueOf(999 - i), batch.get(i));
            }
            assertEquals(936, queue.size());
            assertEquals(Integer.valueOf(0), queue.poll());
            assertEquals(Integer.valueOf(935), queue.removeLast());
        } finally {
            queue.close();
        }
    }

    @Test
    public void concurrencyQueuePollFillsCallerCollection() {
        FileQueue<Integer> queue = FileQueue
            .<Integer>builder()
            .name("test-poll-batch")
            .path(Paths.get("./target/poll-concurrency.queue"))
            .option("concurrency", 4)
            .build();
        new File("./target/poll-concurrency.queue/test-poll-batch").deleteOnExit();
        try {
            for (int i = 0; i < 1000; i++) {
                queue.add(i);
            }
            List<Integer> batch = new ArrayList<>();
            assertEquals(64, queue.poll(64, batch));
            assertEquals(64, batch.size());
            assertEquals(936, queue.size());
            List<Integer> last = new ArrayList<>();
            assertEquals(64, queue.pollLast(64, last));
            assertEquals(64, last.size());
            assertEquals(872, queue.size());
        } finally {
            queue.close();
        }
    }

    @Test
    @SneakyThrows
    public void concurrencyQueueFairPollDrainsEveryShard() {
        Path path = Paths.get("./target/fair-poll-" + System.nanoTime() + ".queue");
        FileQueue<Integer> queue = FileQueue
            .<Integer>builder()
            .name("test-fair-poll")
            .path(path)
            .option("concurrency", 2)
            .build();
        path.resolve("test-fair-poll").toFile().deleteOnExit();
        try {
            Thread firstWriter = new Thread(() -> {
                for (int i = 0; i < 500; i++) {
                    queue.add(i);
                }
            }, "fair-poll-writer-0");
            Thread secondWriter = new Thread(() -> {
                for (int i = 0; i < 500; i++) {
                    queue.add(1000 + i);
                }
            }, "fair-poll-writer-1");
            firstWriter.start();
            secondWriter.start();
            firstWriter.join();
            secondWriter.join();

            assertEquals(1000, queue.size());

            Field queuesField = ConcurrencyMVStoreQueue.class.getDeclaredField("queues");
            queuesField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<MVStoreQueue<Integer>> shards = (List<MVStoreQueue<Integer>>) queuesField.get(queue);
            assertEquals(2, shards.size());
            int shard0 = shards.get(0).size();
            int shard1 = shards.get(1).size();
            if (shard0 != 500 || shard1 != 500) {
                throw new AssertionError(
                    "writer threads landed on one shard: " + shard0 + " + " + shard1 + " (expected 500 + 500)");
            }

            List<Integer> list = new ArrayList<>();
            assertEquals(200, queue.poll(200, list));
            assertEquals(200, list.size());
            assertEquals(400, shards.get(0).size());
            assertEquals(400, shards.get(1).size());
            assertEquals(800, queue.size());

            List<Integer> last = new ArrayList<>();
            assertEquals(200, queue.pollLast(200, last));
            assertEquals(200, last.size());
            assertEquals(300, shards.get(0).size());
            assertEquals(300, shards.get(1).size());
            assertEquals(600, queue.size());
        } finally {
            queue.close();
        }
    }

    @Test
    @SneakyThrows
    public void concurrencyQueueLeftoverSweepFillsFromPopulatedShard() {
        Path path = Paths.get("./target/leftover-poll-" + System.nanoTime() + ".queue");
        ConcurrencyMVStoreQueue<Integer> queue = new ConcurrencyMVStoreQueue<>(
            path,
            "test-leftover",
            Collections.emptyMap(),
            2
        );
        path.resolve("test-leftover").toFile().deleteOnExit();
        try {
            Field queuesField = ConcurrencyMVStoreQueue.class.getDeclaredField("queues");
            queuesField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<MVStoreQueue<Integer>> shards = (List<MVStoreQueue<Integer>>) queuesField.get(queue);
            assertEquals(2, shards.size());
            for (int i = 0; i < 500; i++) {
                shards.get(0).add(i);
            }
            assertEquals(500, shards.get(0).size());
            assertEquals(0, shards.get(1).size());

            List<Integer> list = new ArrayList<>();
            assertEquals(200, queue.poll(200, list));
            assertEquals(200, list.size());
            assertEquals(300, shards.get(0).size());
            assertEquals(0, shards.get(1).size());

            List<Integer> last = new ArrayList<>();
            assertEquals(200, queue.pollLast(200, last));
            assertEquals(200, last.size());
            assertEquals(Integer.valueOf(499), last.get(0));
            assertEquals(100, shards.get(0).size());
            assertEquals(0, shards.get(1).size());
        } finally {
            queue.close();
        }
    }

    @Test
    public void concurrencyQueueSingleShardPollTakesFullShare() {
        Path path = Paths.get("./target/single-shard-" + System.nanoTime() + ".queue");
        ConcurrencyMVStoreQueue<Integer> queue = new ConcurrencyMVStoreQueue<>(
            path,
            "test-single",
            Collections.emptyMap(),
            1
        );
        path.resolve("test-single").toFile().deleteOnExit();
        try {
            for (int i = 0; i < 100; i++) {
                queue.add(i);
            }
            List<Integer> batch = new ArrayList<>();
            assertEquals(64, queue.poll(64, batch));
            assertEquals(64, batch.size());
            for (int i = 0; i < 64; i++) {
                assertEquals(Integer.valueOf(i), batch.get(i));
            }
            assertEquals(36, queue.size());

            List<Integer> last = new ArrayList<>();
            assertEquals(10, queue.pollLast(10, last));
            assertEquals(Integer.valueOf(99), last.get(0));
            assertEquals(26, queue.size());
        } finally {
            queue.close();
        }
    }

    @Test
    @SneakyThrows
    public void concurrencyQueuePeekElementAndPollIgnoreWriteAffinity() {
        Path path = Paths.get("./target/peek-cursor-" + System.nanoTime() + ".queue");
        ConcurrencyMVStoreQueue<Integer> queue = new ConcurrencyMVStoreQueue<>(
            path,
            "test-peek",
            Collections.emptyMap(),
            2
        );
        path.resolve("test-peek").toFile().deleteOnExit();
        try {
            Field queuesField = ConcurrencyMVStoreQueue.class.getDeclaredField("queues");
            queuesField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<MVStoreQueue<Integer>> shards = (List<MVStoreQueue<Integer>>) queuesField.get(queue);
            shards.get(1).add(42);
            assertEquals(Integer.valueOf(42), queue.peek());
            assertEquals(Integer.valueOf(42), queue.element());
            assertEquals(Integer.valueOf(42), queue.poll());
            assertTrue(queue.isEmpty());
        } finally {
            queue.close();
        }
    }

    @Test
    public void fileQueueProxyForwardsBatchPollAndRemoveLast() {
        MVStoreQueue<Integer> queue = new MVStoreQueue<>(
            Paths.get("./target/poll-proxy.queue"),
            "test",
            Collections.emptyMap()
        );
        new File("./target/poll-proxy.queue/test").deleteOnExit();
        AtomicInteger singlePolls = new AtomicInteger();
        FileQueue<Integer> proxy = new FileQueueProxy<Integer>(queue) {
            @Override
            public Integer poll() {
                singlePolls.incrementAndGet();
                return super.poll();
            }
        };
        try {
            for (int i = 0; i < 10; i++) {
                proxy.add(i);
            }
            List<Integer> batch = new ArrayList<>();
            assertEquals(4, proxy.poll(4, batch));
            assertEquals(Arrays.asList(0, 1, 2, 3), batch);
            assertEquals(0, singlePolls.get());

            List<Integer> last = new ArrayList<>();
            assertEquals(3, proxy.pollLast(3, last));
            assertEquals(Arrays.asList(9, 8, 7), last);

            assertEquals(Integer.valueOf(6), proxy.removeLast());
            assertEquals(Integer.valueOf(4), proxy.removeFirst());
            assertEquals(1, proxy.size());
        } finally {
            queue.close();
        }
    }

    @Test
    public void sequentialAddUnderOneThreadStoresIncreasingKeys() {
        MVStoreQueue<Integer> queue = new MVStoreQueue<>(
            Paths.get("./target/sequential-add.queue"),
            "test",
            Collections.emptyMap()
        );
        new File("./target/sequential-add.queue/test").deleteOnExit();
        try {
            for (int i = 0; i < 1000; i++) {
                queue.add(i);
            }
            assertEquals(1000, queue.size());
            for (int i = 0; i < 1000; i++) {
                assertEquals(Integer.valueOf(i), queue.poll());
            }
            assertTrue(queue.isEmpty());
        } finally {
            queue.close();
        }
    }

    @Test
    public void concurrencyQueueIsEmptyScansShards() {
        FileQueue<Integer> queue = FileQueue
            .<Integer>builder()
            .name("test-isempty")
            .path(Paths.get("./target/isempty.queue"))
            .option("concurrency", 4)
            .build();
        new File("./target/isempty.queue/test-isempty").deleteOnExit();
        try {
            assertTrue(queue.isEmpty());
            queue.add(1);
            assertFalse(queue.isEmpty());
            assertEquals(Integer.valueOf(1), queue.poll());
            assertTrue(queue.isEmpty());
        } finally {
            queue.close();
        }
    }

    @Test
    @SneakyThrows
    public void testAddAll() {
        FileQueue<String> strings = FileQueue
            .<String>builder()
            .name("testAddAll")
            .path(Paths.get("./target/.queue"))
            .build();
        new File("./target/.queue/testAddAll").deleteOnExit();

        int size = 10000;
        Flux.range(0, size)
            .flatMap(i -> Mono
                .fromRunnable(() -> {
                    strings.size();
                    strings.addAll(Arrays.asList("1", "2", "3"));
                })
                .subscribeOn(Schedulers.boundedElastic()))
            .then()
            .block();


        assertEquals(size * 3, strings.size());
    }

    @Test
    public void applyStoreOptionsKeepsProductionDefaultsAndAllowsCompressFalse() throws Exception {
        Map<String, Object> fifo = builderConfig(MVStoreQueue.applyStoreOptions(
            new org.h2.mvstore.MVStore.Builder(), Collections.emptyMap(), 16, 32 * 1024));
        assertEquals(16, fifo.get("cacheSize"));
        assertEquals(32 * 1024, fifo.get("autoCommitBufferSize"));
        assertEquals(1, fifo.get("compress"));

        Map<String, Object> concurrent = builderConfig(MVStoreQueue.applyStoreOptions(
            new org.h2.mvstore.MVStore.Builder(), Collections.emptyMap(), 64, 64 * 1024));
        assertEquals(64, concurrent.get("cacheSize"));
        assertEquals(64 * 1024, concurrent.get("autoCommitBufferSize"));
        assertEquals(1, concurrent.get("compress"));

        Map<String, Object> uncompressed = builderConfig(MVStoreQueue.applyStoreOptions(
            new org.h2.mvstore.MVStore.Builder(),
            Collections.singletonMap("compress", false),
            16,
            32 * 1024));
        assertFalse(uncompressed.containsKey("compress"));
        assertEquals(16, uncompressed.get("cacheSize"));
        assertEquals(32 * 1024, uncompressed.get("autoCommitBufferSize"));

        String previous = System.getProperty("jetlinks.filequeue.compress");
        try {
            System.setProperty("jetlinks.filequeue.compress", "false");
            Map<String, Object> fromProperty = builderConfig(MVStoreQueue.applyStoreOptions(
                new org.h2.mvstore.MVStore.Builder(), new HashMap<>(), 16, 32 * 1024));
            assertFalse(fromProperty.containsKey("compress"));
        } finally {
            if (previous == null) {
                System.clearProperty("jetlinks.filequeue.compress");
            } else {
                System.setProperty("jetlinks.filequeue.compress", previous);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> builderConfig(org.h2.mvstore.MVStore.Builder builder) throws Exception {
        Field field = org.h2.mvstore.MVStore.Builder.class.getDeclaredField("config");
        field.setAccessible(true);
        return (Map<String, Object>) field.get(builder);
    }
}
