package org.jetlinks.supports.cluster.event;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.codec.defaults.TopicPayloadCodec;
import org.jetlinks.core.event.TopicPayload;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 支持集群的事件代理
 */
@Slf4j
@Deprecated
public class RedisClusterEventBroker extends AbstractClusterEventBroker {

    public RedisClusterEventBroker(ClusterManager clusterManager, ReactiveRedisConnectionFactory factory) {
        super(clusterManager, factory);
    }

    /**
     * 监听消息
     *
     * @param localId 本地节点ID
     * @param brokerId 集群节点ID
     * @return 监听到的消息
     */
    @Override
    protected Flux<TopicPayload> listen(String localId, String brokerId) {

//        return redis
//                .listenToChannel("/broker/bus/" + brokerId + "/" + localId)
//                .map(msg -> topicPayloadCodec.decode(Payload.of(Unpooled.wrappedBuffer(msg.getMessage()))));
//
        return clusterManager
                .<byte[]>getQueue("/broker/bus/" + brokerId + "/" + localId)
                .subscribe()
                .map(msg -> TopicPayloadCodec.doDecode(Unpooled.wrappedBuffer(msg)));
    }

    /**
     * 消息分发，用于将消息转发到集群中
     *
     * @param localId 本地ID
     * @param brokerId 集群ID
     * @param payload 消息
     * @return void
     */
    @Override
    protected Mono<Void> dispatch(String localId, String brokerId, TopicPayload payload) {
        try {
            ByteBuf byteBuf = TopicPayloadCodec.doEncode(payload);
            byte[] body = ByteBufUtil.getBytes(byteBuf);
            ReferenceCountUtil.safeRelease(payload);
            ReferenceCountUtil.safeRelease(byteBuf);

//        return redis
//                .convertAndSend("/broker/bus/" + localId + "/" + brokerId, body)
//                .then();
            return clusterManager
                    .getQueue("/broker/bus/" + localId + "/" + brokerId)
                    .add(Mono.just(body))
                    .then();
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
        return Mono.empty();
    }
}
