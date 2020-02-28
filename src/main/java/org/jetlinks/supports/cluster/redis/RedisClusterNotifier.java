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
                    EmitterProcessor processor = replyHandlers.get(reply.getMessageId());
                    if (processor != null && !processor.isCancelled()) {
                        if (reply.isSuccess()) {
                            if (reply.isComplete()) {
                                processor.onComplete();
                                replyHandlers.remove(reply.getMessageId());
                                log.debug("complete notify reply [{}:{}]", reply.getAddress(), reply.getMessageId());
                            } else {
                                log.debug("handle notify reply [{}:{}] : {}", reply.getAddress(), reply.getMessageId(), reply.getPayload());
                                processor.onNext(reply.getPayload());
                            }
                        } else {
                            replyHandlers.remove(reply.getMessageId());
                            processor.onError(new NotifyException(reply.getAddress(), reply.getErrorMessage()));
                        }
                    } else {
                        log.warn("no notify[{}] reply [{}] handler : {}", reply.getAddress(), reply.getMessageId(), reply);
                    }
                });
    }

    @Override
    public Mono<Boolean> sendNotify(String serverNodeId, String address, Publisher<?> payload) {

        return Flux.from(payload)
                .map(data -> NotifierMessage.of(UUID.randomUUID().toString(), currentServerId, address, data))
                .doOnNext(notify -> log.debug("send notify [{}] to [{}] : [{}]", address, serverNodeId, notify))
                .as(stream -> clusterManager.<NotifierMessage>getTopic(getNotifyTopicKey(serverNodeId, address)).publish(stream))
                .map(i -> i > 0);
    }

    @Override
    public <T> Flux<T> sendNotifyAndReceive(String serverNodeId, String address, Publisher<?> payload) {
        String messageId = UUID.randomUUID().toString();
        EmitterProcessor<T> processor = EmitterProcessor.create(true);

        replyHandlers.put(messageId, processor);

        return Flux.from(payload)
                .map(data -> NotifierMessage.of(messageId, currentServerId, address, data))
                .doOnNext(notify -> log.debug("send notify [{}] to [{}] : {}", address, serverNodeId, notify))
                .as(stream -> clusterManager.<NotifierMessage>getTopic(getNotifyTopicKey(serverNodeId, address)).publish(stream))
                .flatMap(i -> {
                    if (i < 0) {
                        return Mono.error(new NotifyException(address, "no server handle address notify"));
                    }
                    return Mono.just(i);
                })
                .thenMany(processor.map(Function.identity()))
                .doOnCancel(() -> log.debug("cancel receive notify [{}] reply [{}]", address, messageId))
                .doFinally(f -> replyHandlers.remove(messageId))
                ;

    }

    @Override
    public <T> Flux<T> handleNotify(String address) {
        return clusterManager.<NotifierMessage>getTopic(getNotifyTopicKey(currentServerId, address))
                .subscribe()
                .map(NotifierMessage::getPayload)
                .map(r -> (T) r);

    }

    @Override
    public <T, R> Mono<Void> handleNotify(String address, Function<T, Publisher<R>> replyHandler) {
        return clusterManager
                .<NotifierMessage>getTopic(getNotifyTopicKey(currentServerId, address))
                .subscribe()
                .flatMap(msg -> {
                    String msgId = msg.getMessageId();
                    log.debug("handle notify [{}] from [{}]", address, msg.getFromServer());
                    try {
                        return Flux.from(replyHandler.apply((T) msg.getPayload()))
                                .map(res -> NotifierMessageReply.success(address, msgId, res))
                                .doOnError(error -> log.warn("handle notify error", error))
                                .onErrorResume(err -> Mono.just(NotifierMessageReply.fail(address, msgId, err)))
                                .switchIfEmpty(Mono.just(NotifierMessageReply.success(address, msgId, null)))
                                .flatMap(reply -> {
                                    return clusterManager.<NotifierMessageReply>getTopic(msg.getFromServer().concat(":notify-reply")).publish(Mono.just(reply));
                                })
                                .doOnComplete(() -> {
                                    clusterManager.<NotifierMessageReply>getTopic(msg.getFromServer().concat(":notify-reply"))
                                            .publish(Mono.just(NotifierMessageReply.complete(address, msgId)))
                                            .subscribe();
                                })
                                .doOnNext(len -> {
                                    if (len <= 0) {
                                        log.warn("reply notify [{}] to server[{}] fail ", address, msg.getFromServer());
                                    }
                                });
                    } catch (Exception e) {
                        log.warn("handle notify error", e);
                        return clusterManager.<NotifierMessageReply>getTopic(msg.getFromServer().concat(":notify-reply"))
                                .publish(Mono.just(NotifierMessageReply.fail(address, msgId, e)))
                                .doOnNext(len -> {
                                    if (len <= 0) {
                                        log.warn("reply notify [{}] to server[{}] fail ", address, msg.getFromServer());
                                    }
                                });
                    }
                })
                .onErrorContinue((err, val) -> log.error(err.getMessage(), err))
                .then();

    }
}
