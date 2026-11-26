package org.jetlinks.supports.rpc;

import io.netty.buffer.*;
import org.jetlinks.core.cluster.discovery.*;
import org.jetlinks.core.rpc.transport.RpcFrame;
import org.jetlinks.core.utils.ExceptionSerializer;
import reactor.core.Exceptions;

import java.io.*;
import java.util.*;

/** 仅编码协议元数据；路由前缀在方法绑定时预编码，Context 直接附加二进制。 */
final class RpcProtocol {
    static final int CATALOG = 1, INVOKE = 2, ERROR = 3;
    private static final int MAGIC = 0x4a52, VERSION = 1;
    record Setup(NodeIdentity identity, String namespace, String bodyCodec, String contextCodec, String token) {}
    record Call(ByteBuf route, ByteBuf context) {}

    static void header(ByteBuf output, int kind) { output.writeShort(MAGIC).writeByte(VERSION).writeByte(kind); }
    static int kind(ByteBuf input) {
        if (input.readUnsignedShort() != MAGIC || input.readUnsignedByte() != VERSION) throw new IllegalArgumentException("Unsupported RPC protocol");
        return input.readUnsignedByte();
    }

    static byte[] route(DefaultRpcServiceRegistry.ServiceKey key, String method) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        try {
            header(buffer, INVOKE);
            DataOutputStream output = output(buffer);
            output.writeUTF(key.id()); output.writeUTF(key.name()); output.writeUTF(method);
            return ByteBufUtil.getBytes(buffer);
        } catch (IOException error) { throw Exceptions.propagate(error); }
        finally { buffer.release(); }
    }
    static Call readCall(ByteBuf source) {
        if (peekKind(source) != INVOKE) throw new IllegalArgumentException("Expected RPC invocation");
        int start = source.readerIndex(), end = start + 4, limit = source.writerIndex();
        // Modified-UTF lengths locate the registered byte prefix; no strings or stream adapters are needed.
        for (int i = 0; i < 3; i++) {
            if (limit - end < 2) throw new IllegalArgumentException("Truncated RPC route");
            int length = source.getUnsignedShort(end);
            end += 2;
            if (length > limit - end) throw new IllegalArgumentException("Truncated RPC route");
            end += length;
        }
        if (end == limit) return new Call(source, Unpooled.EMPTY_BUFFER);
        return new Call(source.slice(start, end - start), source.slice(end, limit - end));
    }

    static int peekKind(ByteBuf input) {
        int start = input.readerIndex();
        if (input.readableBytes() < 4 || input.getUnsignedShort(start) != MAGIC || input.getUnsignedByte(start + 2) != VERSION) {
            throw new IllegalArgumentException("Unsupported RPC protocol");
        }
        return input.getUnsignedByte(start + 3);
    }

    static RpcFrame setup(Setup setup) {
        ByteBuf metadata = ByteBufAllocator.DEFAULT.buffer();
        try {
            header(metadata, 0);
            DataOutputStream output = output(metadata);
            output.writeUTF(setup.identity.nodeId());
            output.writeLong(setup.identity.incarnation().getMostSignificantBits());
            output.writeLong(setup.identity.incarnation().getLeastSignificantBits());
            output.writeUTF(setup.namespace);
            output.writeUTF(setup.bodyCodec); output.writeUTF(setup.contextCodec); output.writeUTF(setup.token);
            return new RpcFrame(Unpooled.EMPTY_BUFFER, metadata);
        } catch (Throwable error) { metadata.release(); throw Exceptions.propagate(error); }
    }
    static Setup readSetup(RpcFrame frame) {
        ByteBuf metadata = frame.metadata().duplicate();
        if (kind(metadata) != 0) throw new IllegalArgumentException("Expected RPC setup");
        try {
            DataInputStream input = input(metadata);
            NodeIdentity identity = new NodeIdentity(input.readUTF(), new UUID(input.readLong(), input.readLong()));
            return new Setup(identity, input.readUTF(), input.readUTF(), input.readUTF(), input.readUTF());
        } catch (IOException error) { throw Exceptions.propagate(error); }
    }

    static RpcFrame catalog(NodeIdentity local, DefaultRpcServiceRegistry.Catalog catalog, boolean request) {
        ByteBuf data = ByteBufAllocator.DEFAULT.buffer();
        ByteBuf metadata = request ? ByteBufAllocator.DEFAULT.buffer(4).writeShort(MAGIC).writeByte(VERSION).writeByte(CATALOG) : Unpooled.EMPTY_BUFFER;
        try {
            DataOutputStream output = output(data);
            output.writeUTF(local.nodeId());
            output.writeLong(local.incarnation().getMostSignificantBits()); output.writeLong(local.incarnation().getLeastSignificantBits());
            output.writeLong(catalog.revision()); output.writeInt(catalog.services().size());
            for (var key : catalog.services()) { output.writeUTF(key.id()); output.writeUTF(key.name()); }
            return new RpcFrame(data, metadata);
        } catch (Throwable error) { data.release(); metadata.release(); throw Exceptions.propagate(error); }
    }
    static DefaultRpcServiceRegistry.Catalog readCatalog(RpcFrame frame, NodeIdentity expected) {
        try {
            DataInputStream input = input(frame.data().duplicate());
            NodeIdentity identity = new NodeIdentity(input.readUTF(), new UUID(input.readLong(), input.readLong()));
            if (!expected.equals(identity)) throw new IllegalArgumentException("RPC peer identity changed");
            long revision = input.readLong(); int count = input.readInt();
            if (count < 0 || count > input.available() / 4) throw new IOException("Invalid service catalog count");
            Set<DefaultRpcServiceRegistry.ServiceKey> services = new HashSet<>();
            for (int i = 0; i < count; i++) services.add(new DefaultRpcServiceRegistry.ServiceKey(input.readUTF(), input.readUTF()));
            return new DefaultRpcServiceRegistry.Catalog(revision, Set.copyOf(services));
        } catch (IOException error) { throw Exceptions.propagate(error); }
    }

    // Preserve the platform's error/i18n categories; arbitrary Java exception classes are not a wire contract.
    static RpcFrame error(Throwable error) {
        ByteBuf data = ByteBufAllocator.DEFAULT.buffer();
        try {
            ExceptionSerializer.serialize(error, new ByteBufOutputStream(data), false);
            return new RpcFrame(data, ByteBufAllocator.DEFAULT.buffer(1).writeByte(ERROR));
        } catch (Throwable failure) { data.release(); throw Exceptions.propagate(failure); }
    }
    static void checkResponse(RpcFrame frame) {
        if (frame.metadata().isReadable()) {
            if (frame.metadata().getUnsignedByte(frame.metadata().readerIndex()) != ERROR) throw new IllegalArgumentException("Unsupported RPC response metadata");
            throw Exceptions.propagate(ExceptionSerializer.deserialize(new ByteBufInputStream(frame.data()), false));
        }
    }
    static DataInputStream input(ByteBuf buffer) { return new DataInputStream(new ByteBufInputStream(buffer)); }
    static DataOutputStream output(ByteBuf buffer) { return new DataOutputStream(new ByteBufOutputStream(buffer)); }
}
