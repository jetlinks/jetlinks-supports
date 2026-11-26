package org.jetlinks.supports.rpc;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

/** requestTimeout 为空时不设置业务请求时限；连接及资源预算仍由运行时管理。 */
public record RpcOptions(URI listen, Duration connectTimeout, Duration requestTimeout,
                         Duration reconnectDelay, int maxInFlight, int maxPayloadBytes, String token) {
    public RpcOptions {
        Objects.requireNonNull(listen, "listen"); Objects.requireNonNull(token, "token");
        if (maxInFlight <= 0 || maxPayloadBytes <= 0 || connectTimeout.isZero() || connectTimeout.isNegative()
            || (requestTimeout != null && (requestTimeout.isZero() || requestTimeout.isNegative()))
            || reconnectDelay.isZero() || reconnectDelay.isNegative()) {
            throw new IllegalArgumentException("Invalid RPC resource budget");
        }
    }
    public static RpcOptions defaults(URI listen) {
        return new RpcOptions(listen, Duration.ofSeconds(5), null, Duration.ofSeconds(1),
            1024, 16 * 1024 * 1024, "");
    }
}
