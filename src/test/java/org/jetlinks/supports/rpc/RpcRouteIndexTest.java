package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.jetlinks.core.rpc.annotation.*;
import org.junit.Test;
import reactor.core.publisher.Mono;
import java.io.DataInputStream;
import java.util.Arrays;
import static org.junit.Assert.*;
import static org.jetlinks.supports.rpc.RpcSpiTestSupport.*;

public class RpcRouteIndexTest {
    @RpcApi("测试/\u0000/设备") public interface Api {
        @RpcMethod("读取/温度") Mono<String> read(String value);
    }
    static final class Service implements Api {
        final String name;
        Service(String name) { this.name = name; }
        public Mono<String> read(String value) { return Mono.just(name + value); }
    }
    static DefaultRpcServiceRegistry registry() {
        return new DefaultRpcServiceRegistry(node("local").identity(), (n,k,m,r) -> q -> Mono.empty(), () -> {});
    }
    static byte[] route(String id) {
        return RpcProtocol.route(new DefaultRpcServiceRegistry.ServiceKey(id, RpcMethodBinding.APIS.get(Api.class).name), "读取/温度");
    }

    @Test public void byteIndexPreservesModifiedUtfProtocolAndIndependentReaderIndexes() throws Throwable {
        var registry = registry(); var registration = registry.registerService("标识\u0000", new Service("one:"));
        ByteBuf metadata = Unpooled.directBuffer().writeZero(3).writeBytes(route("标识\u0000")).writeInt(123);
        metadata.readerIndex(3);
        try {
            DataInputStream legacy = new DataInputStream(new ByteBufInputStream(metadata.duplicate()));
            legacy.readInt(); assertEquals("标识\u0000", legacy.readUTF());
            assertEquals("测试/\u0000/设备", legacy.readUTF()); assertEquals("读取/温度", legacy.readUTF());
            var call = RpcProtocol.readCall(metadata);
            assertEquals(3, metadata.readerIndex()); assertEquals(123, call.context().getInt(0));
            assertEquals("one:x", Mono.from(registry.localMethod(call.route()).invoke("x")).block(WAIT));
            assertEquals(1, metadata.refCnt());
        } finally { metadata.release(); registration.dispose(); registry.close(); }
    }

    @Test public void registrationWithdrawalAndReplacementPublishTheSameLocalSnapshot() throws Throwable {
        var registry = registry(); ByteBuf route = Unpooled.wrappedBuffer(route("service"));
        try {
            var first = registry.registerService("service", new Service("first:"));
            assertEquals("first:x", Mono.from(registry.localMethod(route).invoke("x")).block(WAIT));
            first.dispose(); assertTrue(registry.localCatalog().services().isEmpty());
            expectFailure(IllegalStateException.class, () -> registry.localMethod(route));
            var replacement = registry.registerService("service", new Service("second:"));
            first.dispose();
            assertEquals("second:x", Mono.from(registry.localMethod(route).invoke("x")).block(WAIT));
            replacement.dispose(); expectFailure(IllegalStateException.class, () -> registry.localMethod(route));
        } finally { route.release(); registry.close(); }
    }

    @Test public void indexDoesNotRetainOrDependOnTheIncomingBuffer() {
        var registry = registry(); var registration = registry.registerService("service", new Service("one:"));
        try {
            ByteBuf incoming = Unpooled.directBuffer().writeZero(3).writeBytes(route("service"));
            incoming.readerIndex(3);
            var call = RpcProtocol.readCall(incoming);
            assertFalse(call.context().isReadable());
            assertEquals(3, incoming.readerIndex());
            assertNotNull(registry.localMethod(call.route())); incoming.release();
            ByteBuf next = Unpooled.wrappedBuffer(route("service"));
            try { assertNotNull(registry.localMethod(RpcProtocol.readCall(next).route())); }
            finally { next.release(); }
        } finally { registration.dispose(); registry.close(); }
    }

    @Test public void everyTruncatedPrefixAndWrongHeaderAreRejectedWithoutMovingTheInput() {
        byte[] complete = route("service");
        for (int i = 0; i < complete.length; i++) {
            ByteBuf input = Unpooled.wrappedBuffer(Arrays.copyOf(complete, i));
            try { expectFailure(IllegalArgumentException.class, () -> RpcProtocol.readCall(input)); assertEquals(0, input.readerIndex()); }
            finally { input.release(); }
        }
        complete[0] = 0;
        ByteBuf invalid = Unpooled.wrappedBuffer(complete);
        try { expectFailure(IllegalArgumentException.class, () -> RpcProtocol.readCall(invalid)); }
        finally { invalid.release(); }
    }
}
