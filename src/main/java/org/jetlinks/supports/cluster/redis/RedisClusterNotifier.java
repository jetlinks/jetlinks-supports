package org.jetlinks.supports.cluster.redis;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.cluster.ClusterManager;
import org.jetlinks.core.cluster.ClusterNotifier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@SuppressWarnings("all")
@Slf4j
public class RedisClusterNotifier implements ClusterNotifier {

    private String currentServerId;

    private String clusterName;

    private ClusterManager clusterManager;

    private Map<String, EmitterProcessor> replyHandlers = new ConcurrentHashMap<>();

    public RedisClusterNotifier(String clusterName, String currentServerId, ClusterManager clusterManager) {
        this.currentServerId = currentServerId;
        this.clusterManager = clusterManager;
        this.clusterName = clusterName;
    }

    private String getNotifyTopicKey(String serverName, String address) {
        return clusterName.concat("").concat("__notify:").concat(serverName).concat(":").concat(address);
    }

    public void startup() {
        clusterManager.<NotifierMessageReply>getTopic(currentServerId.concat(":notify-reply"))
                .subscribe()
                .subscribe(reply -> {
                    EmitterProcessor processor = replyHandlers.remove(reply.getMessageId());
                    if (processor != null && !processor.isCancelled()) {
                        if (reply.isSuccess()) {
                            if (reply.getPayload() == null) {
                                processor.onComplete();
                            } else {
                                processor.onNext(reply.getPayload());
                            }
                        } else {
                            processor.onError(new NotifyException(reply.getAddress(), reply.getErrorMessage()));
                        }
                    }
                });
    }

    @Override
    public Mono<Boolean> sendNotify(String serverNodeId, String address, Publisher<?> payload) {

        return Flux.from(payload)
                .map(data -> NotifierMessage.of(UUID.randomUUID().toString(), currentServerId, address, data))
                .as(stream -> clusterManager.<NotifierMessage>getTopic(getNotifyTopicKey(serverNodeId, address)).publish(stream))
                .map(i -> i > 0);
    }

    @Override
    public <T> Mono<T> sendNotifyAndReceive(String serverNodeId, String address, Mono<?> payload) {
        String messageId = UUID.randomUUID().toString();
        EmitterProcessor<T> processor = EmitterProcessor.create(true);

        replyHandlers.put(messageId, processor);

        return Flux.from(payload)
                .map(data -> NotifierMessage.of(messageId, currentServerId, address, data))
                .as(stream -> clusterManager.<NotifierMessage>getTopic(getNotifyTopicKey(serverNodeId, address)).publish(stream))
                .flatMap(i -> {
                    if (i < 0) {
                        return Mono.error(new NotifyException(address, "no server handle address notify"));
                    }
                    return Mono.just(i);
                })
                .then(processor.map(Function.identity()).next())
                .doFinally(f -> replyHandlers.remove(messageId));

    }

    @Override
    public <T> Flux<T> handleNotify(String address) {
        return clusterManager.<NotifierMessage>getTopic(getNotifyTopicKey(currentServerId, address))
                .subscribe()
                .map(NotifierMessage::getPayload)
                .map(r -> (T) r);

    }

    @Override
    public <T, R> void handleNotify(String address, Function<T, Mono<R>> replyHandler) {
        clusterManager.<NotifierMessage>getTopic(getNotifyTopicKey(currentServerId, address)).subscribe()
                .subscribe(msg -> {
                    String msgId = msg.getMessageId();
                    log.debug("handle notify [{}] from [{}]", address, msg.getFromServer());
                    replyHandler.apply((T) msg)
                            .map(res -> NotifierMessageReply.success(address, msgId, res))
                            .onErrorResume(err -> Mono.just(NotifierMessageReply.fail(address, msgId, err)))
                            .switchIfEmpty(Mono.just(NotifierMessageReply.success(address, msgId, null)))
                            .flatMap(reply -> {
                                return clusterManager.<NotifierMessageReply>getTopic(msg.getFromServer().concat(":notify-reply")).publish(Mono.just(reply));
                            })
                            .subscribe(len -> {
                                if (len <= 0) {
                                    log.warn("reply notify [{}] to server[{}] fail ", address, msg.getFromServer());
                                }
                            });
                });
    }
}
