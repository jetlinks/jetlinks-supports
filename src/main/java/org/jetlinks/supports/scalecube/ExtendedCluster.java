package org.jetlinks.supports.scalecube;

import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ExtendedCluster extends io.scalecube.cluster.Cluster {

    Flux<MembershipEvent> listenMembership();

    Disposable listenMessage(@Nonnull String qualifier,
                             BiFunction<Message, ExtendedCluster, Mono<Void>> handler);

    Disposable listenGossip(@Nonnull String qualifier,
                            BiFunction<Message, ExtendedCluster, Mono<Void>> handler);

    ExtendedCluster handler(Function<ExtendedCluster, ClusterMessageHandler> handlerFunction);

}
