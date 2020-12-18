package org.jetlinks.supports.rsocket;

import io.rsocket.RSocket;

public interface RSocketService extends RSocket {

    int version();

    String serviceName();

}
