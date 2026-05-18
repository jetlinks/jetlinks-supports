package org.jetlinks.supports.scalecube.rpc;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.hswebframework.web.exception.BusinessException;
import org.hswebframework.web.exception.I18nSupportException;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.jetlinks.supports.scalecube.ExtendedClusterImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.tools.agent.ReactorDebugAgent;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class ScalecubeRpcManagerTest {
    ExtendedCluster cluster1, cluster2, cluster3;
    Member node1, node2, node3;
    ScalecubeRpcManager manager1, manager2, manager3;


    @SneakyThrows
    public static void main(String[] args) {

        ReactorDebugAgent.init();
        ReactorDebugAgent.processExistingClasses();

        try {
            Mono.defer(() -> {
                    return Mono
                        .delay(Duration.ofMillis(1))
                        .thenReturn(1)
                        .flatMap(i -> {
                            return Mono.error(new TimeoutException());
                        })
                        .onErrorResume(Mono::error);
                })
                .retryWhen(ScalecubeRpcManager.DEFAULT_RETRY)
                .block();
        }catch (Throwable e){
            e.printStackTrace();
        }

        System.in.read();


    }

    private ExtendedCluster startCluster(String alias, ExtendedCluster seed) {
        ClusterConfig config = ClusterConfig
            .defaultConfig()
            .transport(conf -> conf.transportFactory(new TcpTransportFactory()))
            .memberAlias(alias);
        if (seed != null) {
            config = config.membership(conf -> conf.seedMembers(seed.address()));
        }
        return new ExtendedClusterImpl(config).startAwait();
    }

    private ScalecubeRpcManager startManager(ExtendedCluster cluster) {
        ScalecubeRpcManager manager = new ScalecubeRpcManager(cluster, RSocketServiceTransport::new);
        manager.startAwait();
        return manager;
    }

    @Before
    public void init() {
        cluster1 = startCluster("node1", null);
        node1 = cluster1.member();
        manager1 = startManager(cluster1);

        cluster2 = startCluster("node2", cluster1);
        node2 = cluster2.member();
        manager2 = startManager(cluster2);

        cluster3 = startCluster("node3", cluster1);
        node3 = cluster3.member();
        manager3 = startManager(cluster3);
    }

    @After
    public void shutdown() {
        if (manager3 != null) {
            manager3.stopAwait();
        }
        if (manager2 != null) {
            manager2.stopAwait();
        }
        if (manager1 != null) {
            manager1.stopAwait();
        }
        if (cluster3 != null && !cluster3.isShutdown()) {
            cluster3.shutdown();
        }
        if (cluster2 != null && !cluster2.isShutdown()) {
            cluster2.shutdown();
        }
        if (cluster1 != null && !cluster1.isShutdown()) {
            cluster1.shutdown();
        }
    }

    @Test
    @SneakyThrows
    public void testNoService() {
        manager1.registerService(new ServiceImpl("1"));

        manager3
            .getService(manager2.currentServerId(),
                        Service.class)
            .as(StepVerifier::create)
            .expectComplete()
            .verify();

        manager2.registerService(new ServiceImpl("2"));

        Thread.sleep(1000);
        manager3
            .getService(manager2.currentServerId(),
                        Service.class)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void testSimple() {

        manager1.registerService(new ServiceImpl("1"));

        manager2.registerService(new ServiceImpl("2"));

        Thread.sleep(2000);

        manager3.getServices(Service.class)
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();

        manager3.getService(node1.alias(), Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("1TEST")
                .verifyComplete();

        manager2.stopAwait();
        Thread.sleep(1000);

        manager3.getServices(Service.class)
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

    }

    @Test
    @SneakyThrows
    public void testError() {
        manager1.registerService("e1", new ServiceImpl("1"));
        manager2.registerService("e1", new ServiceImpl("3"));

        Thread.sleep(2000);

        manager3
            .getServices("e1", Service.class)
            .flatMap(service -> service.service().error())
            .as(StepVerifier::create)
            .expectErrorMatches(err -> {
                err.printStackTrace();
                return "error".equals(err.getMessage());
            })
            .verify();

    }

    @Test
    @SneakyThrows
    public void testCustomId() {
        manager1.registerService("s1", new ServiceImpl("1"));
        manager1.registerService("s2", new ServiceImpl("2"));
        manager2.registerService("s3", new ServiceImpl("3"));
        manager2.registerService("s1", new ServiceImpl("2-1"));

        Thread.sleep(2000);

        manager3.getServices("s1", Service.class)
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();


        manager3.getServices(Service.class)
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();

        manager3.getService(node1.alias(), "s1", Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("1TEST")
                .verifyComplete();

        manager3.getService(node1.alias(), "s2", Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("2TEST")
                .verifyComplete();

    }

    @Test
    public void testEvent() {

        manager3
            .listen(Service.class)
            .doOnSubscribe(s -> {
                Mono.delay(Duration.ofSeconds(1))
                    .subscribe(ignore -> {
                        Disposable disposable = manager1
                            .registerService("t1", new ServiceImpl("t1"));
                        Mono.delay(Duration.ofSeconds(1))
                            .subscribe(i -> {
                                disposable.dispose();
                            });
                    });

            })
            .take(2)
            .timeout(Duration.ofSeconds(4))
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

    }

    @Test
    public void testRegisterTime() {

        manager3
            .listen(Service.class)
            .doOnSubscribe(s -> {
                Mono.delay(Duration.ofSeconds(1))
                    .subscribe(ignore -> {
                        Disposable disposable = manager1
                            .registerService("t1", new ServiceImpl("t1"));
                        Mono.delay(Duration.ofSeconds(1))
                            .subscribe(i -> {
                                disposable.dispose();
                            });
                    });

            })
            .take(2)
            .timeout(Duration.ofSeconds(4))
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

    }

    @Test
    @SneakyThrows
    public void testNative() {
        manager1.registerService("n1", new ServiceImpl("1"));

        Thread.sleep(2000);

        manager3.getService(node1.alias(), "n1", Service.class)
                .flatMapMany(service -> service.read("test"))
                .map(buf -> buf.toString(StandardCharsets.UTF_8))
                .as(StepVerifier::create)
                .expectNext("hel", "lo")
                .verifyComplete();

        manager3.getService(node1.alias(), "n1", Service.class)
                .flatMapMany(service -> service.read0(Unpooled.wrappedBuffer("test".getBytes())))
                .map(buf -> buf.toString(StandardCharsets.UTF_8))
                .as(StepVerifier::create)
                .expectNext("test", "hel", "lo")
                .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void testSelect() {
        manager1.registerService("s1", new ServiceImpl("1"));
        manager2.registerService("s1", new ServiceImpl("1"));

        Thread.sleep(2000);

        manager3
            .selectService(Service.class,
                           Collectors.reducing((a, b) -> {
                               System.out.println(a + "=>" + b);
                               return a;
                           }),
                           Mono.empty())
            .flatMap(s -> s.upper("test-1"))
            .as(StepVerifier::create)
            .expectNext("1TEST-1")
            .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void testSelectServiceWithoutRouteKey() {
        manager1.registerService("s1", new ServiceImpl("1"));
        manager2.registerService("s1", new ServiceImpl("2"));

        Thread.sleep(2000);

        manager3.selectService(Service.class)
                .flatMap(service -> service.service().upper("test"))
                .as(StepVerifier::create)
                .expectNextMatches(result -> "1TEST".equals(result) || "2TEST".equals(result))
                .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void testNodeRestartShouldRecoverRouteWithoutGatewayRestart() {
        manager2.registerService("s1", new ServiceImpl("2"));

        Thread.sleep(2000);

        manager3.getService(node2.alias(), "s1", Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("2TEST")
                .verifyComplete();

        manager2.stopAwait();
        cluster2.shutdown();

        Thread.sleep(2000);

        manager3.getService(node2.alias(), "s1", Service.class)
                .as(StepVerifier::create)
                .verifyComplete();

        cluster2 = startCluster("node2", cluster1);
        node2 = cluster2.member();
        manager2 = startManager(cluster2);
        manager2.registerService("s1", new ServiceImpl("2-1"));

        Thread.sleep(2000);

        manager3.getService(node2.alias(), "s1", Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("2-1TEST")
                .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void testAliasReuseShouldRefreshServiceReferenceAndIgnoreStaleLeave() {
        manager2.registerService("s1", new ServiceImpl("old"));

        Thread.sleep(2000);

        manager3.getService(node2.alias(), "s1", Service.class)
                .flatMap(service -> service.upper("test"))
                .as(StepVerifier::create)
                .expectNext("oldTEST")
                .verifyComplete();

        ExtendedCluster replacementCluster = startCluster("node2", null);
        ScalecubeRpcManager replacementManager = startManager(replacementCluster);
        try {
            replacementManager.registerService("s1", new ServiceImpl("new"));
            manager3.handleServiceEndpoint(replacementCluster.member(), replacementManager.createEndpoint());

            manager3.getService(node2.alias(), "s1", Service.class)
                    .flatMap(service -> service.upper("test"))
                    .as(StepVerifier::create)
                    .expectNext("newTEST")
                    .verifyComplete();

            manager3.memberLeave(node2);

            manager3.getService(node2.alias(), "s1", Service.class)
                    .flatMap(service -> service.upper("test"))
                    .as(StepVerifier::create)
                    .expectNext("newTEST")
                    .verifyComplete();
        } finally {
            replacementManager.stopAwait();
            replacementCluster.shutdown();
        }
    }

    /**
     * 递归调用测试：node1 -> node2 -> node3 -> node1 -> ...，验证 maxCallDepth 最大递归次数限制。
     * 不设业务层 step 终止条件，依赖 ScalecubeRpcManager 的 maxCallDepth 在超过时抛出异常。
     */
    @Test
    @SneakyThrows
    public void testRecursiveChainCallMaxDepthLimit() {
        int maxDepth = 3;
        manager1.setMaxCallDepth(maxDepth);
        manager2.setMaxCallDepth(maxDepth);
        manager3.setMaxCallDepth(maxDepth);

        manager1.registerService("chain", new ChainServiceImpl(manager1, "node2", "node1"));
        manager2.registerService("chain", new ChainServiceImpl(manager2, "node3", "node2"));
        manager3.registerService("chain", new ChainServiceImpl(manager3, "node1", "node3"));

        Thread.sleep(2000);

        manager3
            .getService("node1", "chain", ChainService.class)
            .flatMap(ChainService::chainCall)
            .as(StepVerifier::create)
            .expectErrorMatches(err -> err instanceof I18nSupportException
                && "error.out_of_max_call_depth".equals(((I18nSupportException) err).getI18nCode()))
            .verify();
    }

    /**
     * 用于递归/链式调用测试的接口：node1 -> node2 -> node3 -> node1
     */
    @io.scalecube.services.annotations.Service
    public interface ChainService {

        @ServiceMethod
        Mono<String> chainCall();
    }

    /**
     * 链式调用实现：始终调用下一节点，不设业务层终止条件，由框架 maxCallDepth 限制截断。
     */
    @AllArgsConstructor
    public static class ChainServiceImpl implements ChainService {

        private final ScalecubeRpcManager manager;
        private final String nextNodeAlias;
        private final String myAlias;

        @Override
        public Mono<String> chainCall() {
            return manager
                .getService(nextNodeAlias, "chain", ChainService.class)
                .flatMap(ChainService::chainCall)
                .map(nextResult -> myAlias + "->" + nextResult);
        }
    }

    @io.scalecube.services.annotations.Service
    public interface Service {

        @ServiceMethod
        Mono<String> upper(String value);

        @ServiceMethod
        Flux<ByteBuf> read(String id);

        @ServiceMethod
        Flux<ByteBuf> read0(ByteBuf buf);

        @ServiceMethod
        Mono<String> error();
    }

    @AllArgsConstructor
    public static class ServiceImpl implements Service {

        private final String prefix;

        @Override
        public Mono<String> error() {
            return Mono
                .<String>defer(() -> Mono.error(new BusinessException("error")))
                .as(MonoTracer.create("/test"));
        }

        @Override
        public Mono<String> upper(String value) {
            return Mono.just(prefix + (value.toUpperCase(Locale.ROOT)));
        }

        @Override
        public Flux<ByteBuf> read0(ByteBuf buf) {
            return Flux.just(buf,
                             Unpooled.wrappedBuffer("hel".getBytes()),
                             Unpooled.wrappedBuffer("lo".getBytes()));
        }

        @Override
        public Flux<ByteBuf> read(String id) {
//            return Flux.just("1","2");
            return Flux.just(Unpooled.wrappedBuffer("hel".getBytes()),
                             Unpooled.wrappedBuffer("lo".getBytes()));
        }
    }

}
