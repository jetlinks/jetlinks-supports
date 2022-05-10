package org.jetlinks.supports.device.session;

import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.annotations.ServiceMethod;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.utils.Reactors;
import org.jetlinks.supports.scalecube.EmptyServiceMethodRegistry;
import org.jetlinks.supports.scalecube.ExtendedCluster;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * 微服务设备会话管理器
 *
 * @author zhouhao
 */
@Slf4j
public class MicroserviceDeviceSessionManager extends AbstractDeviceSessionManager {

    private final Map<String, Service> services = new ConcurrentHashMap<>();

    private static final String SERVER_ID_TAG = "msid";

    private static final String SERVER_HELLO = "session-manager-hello";

    private final ExtendedCluster cluster;

    private final ServiceCall serviceCall;

    public MicroserviceDeviceSessionManager(ExtendedCluster cluster,
                                            ServiceCall serviceCall) {
        this.cluster = cluster;
        this.serviceCall = serviceCall;
        this.cluster.handler(ignore -> new ServiceHandler());
        sayHello();
    }

    /**
     * 设备管理服务
     *
     * <P>代理模式的使用</P>
     *
     * <li>{@link Service} 接口定义</li>
     * <li>{@link ServiceImpl} 真实实现类</li>
     * <li>{@link Service} 代理实现类</li>
     *
     */
    @io.scalecube.services.annotations.Service
    public interface Service {

        /**
         * 设备会话是否存活
         *
         * @param deviceId 设备ID
         * @return 设备会话是否存活
         */
        @ServiceMethod
        Mono<Boolean> isAlive(String deviceId);

        /**
         * 会话总数
         *
         * @return 会话总数
         */
        @ServiceMethod
        Mono<Long> total();

        /**
         * 初始化设备
         *
         * @param deviceId 设备ID
         * @return 初始化是否成功
         */
        @ServiceMethod
        Mono<Boolean> init(String deviceId);

        /**
         * 删除设备会话
         *
         * @param deviceId 设备ID
         * @return 被删除的会话数量
         */
        @ServiceMethod
        Mono<Long> remove(String deviceId);
    }

    /**
     * <P>代理模式的使用</P>
     * <li>{@link Service} 接口定义</li>
     * <li>{@link ServiceImpl} 真实实现类</li>
     * <li>{@link Service} 代理实现类</li>
     */
    @AllArgsConstructor
    public static class ServiceImpl implements Service {

        /**
         * 设备会话管理器提供商
         */
        private final Supplier<AbstractDeviceSessionManager> managerSupplier;

        /**
         * 统一处理方法
         *
         * @param arg0         参数0
         * @param arg          二元处理函数
         * @param defaultValue 默认返回值
         * @return             处理结果
         * @param <T>          返回值类型
         * @param <Arg0>       参数0类型
         */
        private <T, Arg0> T doWith(Arg0 arg0,
                                   BiFunction<AbstractDeviceSessionManager, Arg0, T> arg,
                                   T defaultValue) {
            AbstractDeviceSessionManager manager = managerSupplier.get();
            if (manager == null) {
                return defaultValue;
            }
            return arg.apply(manager, arg0);
        }

        /**
         * 设备会话是否存活
         *
         * @param deviceId 设备ID
         * @return 设备会话是否存活
         */
        @Override
        public Mono<Boolean> isAlive(String deviceId) {
            return doWith(deviceId,
                          (manager, id) -> manager.isAlive(id, true),
                          Reactors.ALWAYS_FALSE);
        }

        /**
         * 会话总数
         *
         * @return 会话总数
         */
        @Override
        public Mono<Long> total() {
            return doWith(null,
                          (manager, nil) -> manager.totalSessions(true),
                          Reactors.ALWAYS_ZERO_LONG);
        }

        /**
         * 初始化设备
         *
         * @param deviceId 设备ID
         * @return 初始化是否成功
         */
        @Override
        public Mono<Boolean> init(String deviceId) {
            return doWith(deviceId,
                          AbstractDeviceSessionManager::doInit,
                          Reactors.ALWAYS_FALSE);
        }

        /**
         * 删除设备会话
         *
         * @param deviceId 设备ID
         * @return 被删除的会话数量
         */
        @Override
        public Mono<Long> remove(String deviceId) {
            return doWith(deviceId,
                          AbstractDeviceSessionManager::removeFromCluster,
                          Reactors.ALWAYS_ZERO_LONG);
        }
    }

    /**
     * 初始化会话管理器任务（定时检查设备状态）
     * <p>定时集群间传播消息</p>
     */
    @Override
    public void init() {
        super.init();

        disposable.add(
                Flux.interval(Duration.ofSeconds(30))
                    .doOnNext(ignore -> this.sayHello())
                    .subscribe()
        );
    }

    /**
     * 集群之间传播消息
     */
    private void sayHello() {
        cluster.spreadGossip(Message
                                     .builder()
                                     .qualifier(SERVER_HELLO)
                                     .data(getCurrentServerId())
                                     .build())
               .subscribe();
    }

    /**
     * 集群服务处理程序
     */
    class ServiceHandler implements ClusterMessageHandler {
        @Override
        public void onGossip(Message gossip) {
            if (Objects.equals(SERVER_HELLO, gossip.qualifier())) {
                String id = gossip.data();
                services.put(id, createService(id));
            }
        }

        @Override
        public void onMessage(Message message) {

        }

        @Override
        public void onMembershipEvent(MembershipEvent event) {
            if (event.isRemoved() || event.isLeaving()) {
                Member member = event.member();
                services.remove(member.id());
                if (StringUtils.hasText(member.alias())) {
                    services.remove(member.alias());
                }
            }
        }
    }

    /**
     * 代理模式的使用
     *
     * <li>{@link Service} 接口定义</li>
     * <li>{@link ServiceImpl} 真实实现类</li>
     * <li>{@link Service} 代理实现类</li>
     */
    @AllArgsConstructor
    static class ErrorHandleService implements Service {
        private final String id;
        private final Service service;

        /**
         * 异常处理
         *
         * @param error
         */
        private void handleError(Throwable error) {
            log.warn("cluster[{}] session manager is failed", id, error);
        }

        /**
         * 设备会话是否存活
         *
         * @param deviceId 设备ID
         * @return 设备会话是否存活
         */
        @Override
        public Mono<Boolean> isAlive(String deviceId) {
            return service
                    .isAlive(deviceId)
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_FALSE;
                    });
        }

        /**
         * 会话总数
         *
         * @return 会话总数
         */
        @Override
        public Mono<Long> total() {
            return service
                    .total()
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_ZERO_LONG;
                    });
        }

        /**
         * 初始化设备
         *
         * @param deviceId 设备ID
         * @return 初始化是否成功
         */
        @Override
        public Mono<Boolean> init(String deviceId) {
            return service
                    .init(deviceId)
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_FALSE;
                    });
        }

        /**
         * 删除设备会话
         *
         * @param deviceId 设备ID
         * @return 被删除的会话数量
         */
        @Override
        public Mono<Long> remove(String deviceId) {
            return service
                    .remove(deviceId)
                    .onErrorResume(err -> {
                        handleError(err);
                        return Reactors.ALWAYS_ZERO_LONG;
                    });
        }
    }

    /**
     * 创建服务并注册
     *
     * @param id 服务ID
     * @return 服务
     */
    private Service createService(String id) {
        Service remote = serviceCall
                // 设置路由
                .router((serviceRegistry, request) -> {
                    if (serviceRegistry == null) {
                        return Optional.empty();
                    }
                    // 根据服务ID查找服务引用
                    for (ServiceReference serviceReference : serviceRegistry.lookupService(request)) {
                        if (Objects.equals(id, serviceReference.tags().get(SERVER_ID_TAG))) {
                            return Optional.of(serviceReference);
                        }
                    }
                    return Optional.empty();
                })
                // 服务方法注册
                .methodRegistry(EmptyServiceMethodRegistry.INSTANCE)
                .api(Service.class);
        // 猜测这个地方的remote的类型 应该是ServiceImpl 然后用 ErrorHandleService 包装一下 使得Service 具备异常处理能力
        return new ErrorHandleService(id, remote);
    }

    /**
     * 创建服务节点
     *
     * @param serverId        服务节点ID
     * @param managerSupplier 管理器供应商
     * @return 服务节点信息
     */
    public static ServiceInfo createService(String serverId,
                                            Supplier<AbstractDeviceSessionManager> managerSupplier) {
        return ServiceInfo
                .fromServiceInstance(new ServiceImpl(managerSupplier))
                .tag(SERVER_ID_TAG, serverId)
                .build();
    }

    /**
     * 当前集群节点ID
     *
     * @return 集群节点ID
     */
    @Override
    public final String getCurrentServerId() {
        String id = cluster.member().alias();

        return id == null ? cluster.member().id() : id;
    }

    /**
     * 初始化设备会话连接
     *
     * @param session 设备会话
     * @return
     */
    @Override
    protected final Mono<Boolean> initSessionConnection(DeviceSession session) {
        if (services.size() == 0) {
            return Reactors.ALWAYS_FALSE;
        }
        return getServices()
                .concatMap(service -> service.init(session.getDeviceId()))
                .takeUntil(Boolean::booleanValue)
                .any(Boolean::booleanValue);
    }

    /**
     * 移除远程会话
     *
     * @param deviceId 设备ID
     * @return
     */
    @Override
    protected final Mono<Long> removeRemoteSession(String deviceId) {
        if (services.size() == 0) {
            return Reactors.ALWAYS_ZERO_LONG;
        }
        return getServices()
                .flatMap(service -> service.remove(deviceId))
                .reduce(Math::addExact);
    }

    /**
     * 获取远程会话总数
     *
     * @return 远程会话总数
     */
    @Override
    protected final Mono<Long> getRemoteTotalSessions() {
        if (services.size() == 0) {
            return Reactors.ALWAYS_ZERO_LONG;
        }
        return this
                .getServices()
                .flatMap(Service::total)
                .reduce(Math::addExact);
    }

    /**
     * 监测远程会话是否存活
     *
     * @param deviceId 设备ID
     * @return 是否存活
     */
    @Override
    protected final Mono<Boolean> remoteSessionIsAlive(String deviceId) {
        if (services.size() == 0) {
            return Reactors.ALWAYS_FALSE;
        }
        return getServices()
                .flatMap(service -> service.isAlive(deviceId))
                .any(Boolean::booleanValue)
                .defaultIfEmpty(false);
    }

    /**
     * 获取所有节点
     *
     * @return 所有节点
     */
    private Flux<Service> getServices() {
        return Flux.fromIterable(services.values());
    }
}
