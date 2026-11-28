package org.jetlinks.supports.rpc;

import org.junit.Test;
import java.net.URI;
import java.time.Duration;
import static org.junit.Assert.*;

public class RpcOptionsTest {
    private static final URI LISTEN = URI.create("tcp://127.0.0.1:0");
    private static RpcOptions requestTimeout(Duration timeout) {
        RpcOptions defaults = RpcOptions.defaults(LISTEN);
        return new RpcOptions(defaults.listen(), defaults.connectTimeout(), timeout, defaults.reconnectDelay(),
            defaults.maxInFlight(), defaults.maxPayloadBytes(), defaults.token());
    }
    @Test public void businessOwnsRequestTimeoutByDefault() {
        assertNull(RpcOptions.defaults(LISTEN).requestTimeout());
        assertNull(requestTimeout(null).requestTimeout());
        assertEquals(Duration.ofSeconds(5), RpcOptions.defaults(LISTEN).connectTimeout());
    }
    @Test public void positiveFrameworkRequestTimeoutRemainsOptional() {
        assertEquals(Duration.ofSeconds(3), requestTimeout(Duration.ofSeconds(3)).requestTimeout());
    }
    @Test public void zeroAndNegativeRequestTimeoutsAreRejected() {
        for (Duration timeout : new Duration[]{Duration.ZERO, Duration.ofNanos(-1)}) {
            try { requestTimeout(timeout); fail("invalid request timeout"); }
            catch (IllegalArgumentException expected) { }
        }
    }
}
