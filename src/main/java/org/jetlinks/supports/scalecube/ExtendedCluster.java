package org.jetlinks.supports.scalecube;

import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 集群管理器
 *
 * @author zhouhao
 * @since 1.20
 */
public interface ExtendedCluster extends io.scalecube.cluster.Cluster {

    /**
     * 监听集群节点事件
     *
     * @return 集群节点事件
     */
    Flux<MembershipEvent> listenMembership();

    /**
     * 监听集群消息点对点可通过返回值{@link  Disposable#dispose()}来取消监听
     *
     * @param qualifier 消息标识
     * @param handler   消息处理器
     * @return Disposable
     */
    Disposable listenMessage(@Nonnull String qualifier,
                             BiFunction<Message, ExtendedCluster, Mono<Void>> handler);

    /**
     * 监听集群广播消息,可通过返回值{@link  Disposable#dispose()}来取消监听
     *
     * @param qualifier 消息标识
     * @param handler   消息处理器
     * @return Disposable
     */
    Disposable listenGossip(@Nonnull String qualifier,
                            BiFunction<Message, ExtendedCluster, Mono<Void>> handler);

    /**
     * 设置集群消息监听器
     *
     * @param handlerFunction 监听器构造函数
     * @return this
     */
    ExtendedCluster handler(Function<ExtendedCluster, ClusterMessageHandler> handlerFunction);

    /**
     * 设置集群消息监听器
     *
     * @param handler 监听器
     * @return this
     */
    ExtendedCluster handler(ClusterMessageHandler handler);

    /**
     * 注册当前节点的feature,可用于标识当前服务支持的功能.
     *
     * @param features feature
     */
    void registerFeatures(Collection<String> features);

    /**
     * 获取支持feature的节点信息,可用于获取支持某些功能的集群节点进行某些操作.
     *
     * @param featureId feature
     * @return 节点
     */
    List<Member> featureMembers(String featureId);

    /**
     * 判断某个节点是否支持feature.
     *
     * @param member    节点Id
     * @param featureId featureId
     * @return 是否支持
     */
    boolean supportFeature(String member, String featureId);
}
