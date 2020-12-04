package org.jetlinks.supports.rsocket;

import io.rsocket.RSocket;

public interface ServerNode extends RSocket {

    String getNodeId();


}
