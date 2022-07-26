package org.jetlinks.supports.things;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.jetlinks.core.things.ThingProperty;
import org.jetlinks.core.things.ThingType;
import org.jetlinks.core.things.ThingsDataManager;
import org.jetlinks.core.utils.SerializeUtils;
import org.jetlinks.core.utils.StringBuilderUtils;
import reactor.core.publisher.Mono;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class LocalFileThingsDataManager implements ThingsDataManager {

    private final MVStore mvStore;

    private final MVMap<String, Integer> tagMap;

    private final MVMap<Long, ThingPropertyStore> store;
    private final static int DEFAULT_MAX_STORE_SIZE_EACH_KEY = 8;


    private static MVStore open(String fileName) {
        return new MVStore.Builder()
                .fileName(fileName)
                .autoCommitBufferSize(64 * 1024)
                .compressHigh()
                .keysPerPage(1024)
                .cacheSize(64)
                .open();
    }

    @SuppressWarnings("all")
    private static MVStore load(String fileName) {
        File file = new File(fileName);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try {
            return open(fileName);
        } catch (Throwable err) {
            if (file.exists()) {
                file.renameTo(new File(fileName + "_load_err_" + System.currentTimeMillis()));
                file.delete();
                return open(fileName);
            } else {
                throw err;
            }
        }
    }

    public LocalFileThingsDataManager(String fileName) {
        this(load(fileName));
    }

    public LocalFileThingsDataManager(MVStore store) {
        this.mvStore = store;
        this.tagMap = mvStore.openMap("tags");
        this.store = mvStore
                .openMap("store", new MVMap
                        .Builder<Long, ThingPropertyStore>()
                        .valueType(new StoreType()));
    }


    public void shutdown() {
        mvStore.compactMoveChunks();
        mvStore.close(60_000);
    }

    @Override
    public final Mono<ThingProperty> getLastProperty(ThingType thingType,
                                                     String thingId,
                                                     String property,
                                                     long baseTime) {

        ThingPropertyStore propertyStore = store.get(getKey(thingType.getId(), thingId, property));
        if (propertyStore == null) {
            return lastPropertyNotFound(thingType, thingId, property, baseTime);
        }
        PropertyStore pro = propertyStore.getProperty(baseTime);
        if (pro == null) {
            return lastPropertyNotFound(thingType, thingId, property, baseTime);
        }
        return pro.toProperty(property);
    }

    protected Mono<ThingProperty> lastPropertyNotFound(ThingType thingType,
                                                       String thingId,
                                                       String property,
                                                       long baseTime) {
        return Mono.empty();
    }

    @Override
    public Mono<ThingProperty> getFirstProperty(ThingType thingType,
                                                String thingId,
                                                String property) {
        ThingPropertyStore propertyStore = store.get(getKey(thingType.getId(), thingId, property));
        if (propertyStore == null) {
            return Mono.empty();
        }
        PropertyStore pro = propertyStore.first;
        if (pro == null) {
            return Mono.empty();
        }
        return pro.toProperty(property);
    }


    @Override
    public Mono<Long> getLastPropertyTime(ThingType thingType, String thingId, long baseTime) {
        return Mono.empty();
    }

    @Override
    public Mono<Long> getFirstPropertyTime(ThingType thingType, String thingId) {
        return Mono.empty();
    }

    private int getTag(String key) {
        return tagMap.computeIfAbsent(key, k -> tagMap.size() + 1);
    }

    @SneakyThrows
    protected ObjectInput createInput(ByteBuf buffer) {
        return new ObjectInputStream(new ByteBufInputStream(buffer, true));
    }

    @SneakyThrows
    protected ObjectOutput createOutput(ByteBuf buffer) {
        return new ObjectOutputStream(new ByteBufOutputStream(buffer));
    }

    protected final void updateProperty(String thingType, String thingId, ThingProperty property) {
        updateProperty(thingType, thingId, property.getProperty(), property.getTimestamp(), property.getValue(), property.getState());
    }

    protected long getKey(String thingType, String thingId, String property) {

        int thingTag = getTag(StringBuilderUtils.buildString(
                thingType, thingId,
                (a, b, sb) -> sb.append(a).append(':').append(b)));

        int propertyTag = getTag(property);

        return ((long) thingTag << 32) + propertyTag;
    }

    protected final void updateProperty(String thingType, String thingId, String property, long timestamp, Object value, String state) {
        long key = getKey(thingType, thingId, property);
        ThingPropertyStore propertyStore = store.computeIfAbsent(key, (ignore) -> new ThingPropertyStore());

        PropertyStore s = new PropertyStore();
        s.setTime(timestamp);
        s.setValue(value);
        s.setState(state);
        propertyStore.update(s);
        store.put(key, propertyStore);

    }

    public static class ThingPropertyStore implements Externalizable {
        private PropertyStore first;

        private PropertyStore[] refs;
        private long minTime = -1;

        public PropertyStore getProperty(long baseTime) {
            if (refs == null) {
                return null;
            }
            for (PropertyStore ref : refs) {
                if (ref != null && ref.time <= baseTime) {
                    return ref;
                }
            }
            return null;
        }

        public void update(PropertyStore ref) {
            if (refs == null) {
                refs = new PropertyStore[0];
            }
            if (first == null || first.time >= ref.time) {
                first = ref;
            }

            if (minTime > 0) {
                //时间回退?
                if (ref.time < minTime) {
                    return;
                }
            }

            boolean newEl = false;
            if (refs.length < DEFAULT_MAX_STORE_SIZE_EACH_KEY) {
                refs = Arrays.copyOf(refs, refs.length + 1);
                newEl = true;
            }

            PropertyStore last = refs[0];
            //fast
            if (last == null || ref.time >= last.time || newEl) {
                refs[refs.length - 1] = ref;
            }
            //slow
            else {
                for (int i = 1; i < refs.length; i++) {
                    last = refs[i];
                    if (ref.time == last.time) {
                        refs[i] = ref;
                    } else if (ref.time > last.time) {
                        System.arraycopy(refs, i, refs, i + 1, refs.length - i - 1);
                        refs[i] = ref;
                        break;
                    }
                }
            }

            Arrays.sort(refs, Comparator.comparingLong(r -> r == null ? 0 : -r.time));
            minTime = refs[refs.length - 1].time;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeShort(refs.length);
            for (PropertyStore ref : refs) {
                ref.writeExternal(out);
            }

            out.writeBoolean(first != null);
            if (first != null) {
                first.writeExternal(out);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int len = in.readShort();

            refs = new PropertyStore[len];
            for (int i = 0; i < len; i++) {
                refs[i] = new PropertyStore();
                refs[i].readExternal(in);
            }

            if (in.readBoolean()) {
                first = new PropertyStore();
                first.readExternal(in);
            }
        }

        public int memory() {
            int i = 0;
            if (first != null) {
                i += first.memory();
            }
            if (refs != null) {
                for (PropertyStore ref : refs) {
                    if (ref != null) {
                        i += ref.memory();
                    }
                }
            }
            return i;
        }
    }

    @Getter
    @Setter
    public static class PropertyStore implements Externalizable {
        private long time;
        private String state;
        private Object value;

        private Mono<ThingProperty> _temp;

        public Mono<ThingProperty> toProperty(String property) {
            if (_temp == null) {
                _temp = Mono.just(ThingProperty.of(property, value, time, state));
            }
            return _temp;
        }

        public int memory() {
            int i = 8;
            if (state != null) {
                i += state.length() * 2;
            }
            if (value instanceof Number) {
                i += 8;
            } else {
                i += 64;
            }
            return i;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(time);
            SerializeUtils.writeObject(state, out);
            SerializeUtils.writeObject(value, out);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            time = in.readLong();
            state = (String) SerializeUtils.readObject(in);
            value = SerializeUtils.readObject(in);
        }
    }

    private class StoreType extends BasicDataType<ThingPropertyStore> {

        @Override
        public int compare(ThingPropertyStore a, ThingPropertyStore b) {
            if (a.refs == null && b.refs == null) {
                return 0;
            }
            if (a.refs == null) {
                return -1;
            }
            if (b.refs == null) {
                return 1;
            }
            return Long.compare(a.refs[0].time, b.refs[0].time);
        }

        @Override
        public int getMemory(ThingPropertyStore obj) {
            return obj.memory();
        }

        @Override
        @SneakyThrows
        public void write(WriteBuffer buff, ThingPropertyStore data) {
            ByteBuf buffer = Unpooled.buffer();
            try (ObjectOutput output = createOutput(buffer)) {
                data.writeExternal(output);
            }
            buff.put(buffer.nioBuffer());
            ReferenceCountUtil.safeRelease(buffer);
        }

        @Override
        @SneakyThrows
        public void write(WriteBuffer buff, Object obj, int len) {
            ByteBuf buffer = Unpooled.buffer();
            try (ObjectOutput output = createOutput(buffer)) {
                for (int i = 0; i < len; i++) {
                    ((ThingPropertyStore) Array.get(obj, i)).writeExternal(output);
                }
            }

            buff.put(buffer.nioBuffer());
            ReferenceCountUtil.safeRelease(buffer);
        }

        @Override
        @SneakyThrows
        public void read(ByteBuffer buff, Object obj, int len) {
            try (ObjectInput input = createInput(Unpooled.wrappedBuffer(buff))) {
                for (int i = 0; i < len; i++) {
                    ThingPropertyStore data = new ThingPropertyStore();
                    data.readExternal(input);
                    Array.set(obj, i, data);
                }
            }
        }

        @Override
        public ThingPropertyStore[] createStorage(int size) {
            return new ThingPropertyStore[size];
        }

        @Override
        @SneakyThrows
        public ThingPropertyStore read(ByteBuffer buff) {
            ThingPropertyStore data = new ThingPropertyStore();
            try (ObjectInput input = createInput(Unpooled.wrappedBuffer(buff))) {
                data.readExternal(input);
            }
            return data;
        }

    }
}
