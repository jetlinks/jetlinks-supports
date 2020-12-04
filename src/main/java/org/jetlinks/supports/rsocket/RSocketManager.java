package org.jetlinks.supports.rsocket;

import reactor.core.publisher.Mono;

import java.util.List;

public interface RSocketManager {

    Mono<ServerNode> getServer(String serverId);

    List<ServerNode> getServers();


}
