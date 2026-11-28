package org.jetlinks.supports.cluster.discovery;

import org.jetlinks.core.cluster.discovery.*;
import java.io.*;
import java.net.URI;
import java.util.*;

/** 公告的显式基础类型编码，避免节点模型依赖业务对象序列化器。 */
public final class NodeAnnouncementCodec {
    private NodeAnnouncementCodec() {}
    public static void write(NodeAnnouncement node, DataOutput output) throws IOException {
        output.writeUTF(node.identity().nodeId());
        output.writeLong(node.identity().incarnation().getMostSignificantBits());
        output.writeLong(node.identity().incarnation().getLeastSignificantBits());
        output.writeUTF(node.namespace());
        output.writeUTF(node.application());
        output.writeInt(node.labels().size());
        for (var entry : node.labels().entrySet()) { output.writeUTF(entry.getKey()); output.writeUTF(entry.getValue()); }
        output.writeInt(node.endpoints().size());
        for (var entry : node.endpoints().entrySet()) { output.writeUTF(entry.getKey()); output.writeUTF(entry.getValue().toString()); }
    }
    public static NodeAnnouncement read(DataInputStream input) throws IOException {
        NodeIdentity identity = new NodeIdentity(input.readUTF(), new UUID(input.readLong(), input.readLong()));
        String namespace = input.readUTF(), application = input.readUTF();
        Map<String, String> labels = new LinkedHashMap<>();
        int count = count(input);
        for (int i = 0; i < count; i++) labels.put(input.readUTF(), input.readUTF());
        Map<String, URI> endpoints = new LinkedHashMap<>();
        count = count(input);
        for (int i = 0; i < count; i++) endpoints.put(input.readUTF(), URI.create(input.readUTF()));
        return new NodeAnnouncement(identity, namespace, application, labels, endpoints);
    }
    private static int count(DataInputStream input) throws IOException {
        int value = input.readInt();
        if (value < 0 || value > input.available() / 4) throw new IOException("Invalid announcement entry count");
        return value;
    }
}
