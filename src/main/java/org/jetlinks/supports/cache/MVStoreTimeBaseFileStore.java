package org.jetlinks.supports.cache;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jctools.maps.NonBlockingHashMap;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

@Slf4j
public class MVStoreTimeBaseFileStore<T extends Serializable> implements TimeBaseFileStore<T> {

    private final static int DEFAULT_MAX_STORE_SIZE_EACH_KEY = 8;

    private MVStore store;

    private final Map<String, MVMap<String, Refs>> cache = new NonBlockingHashMap<>();

    @Setter
    private int maxStoreSizeEachKey = DEFAULT_MAX_STORE_SIZE_EACH_KEY;

    public MVStoreTimeBaseFileStore(String fileName) {
        File file = new File(fileName);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try {
            this.store = MVStore.open(fileName);
        } catch (Throwable err) {
            if (file.exists()) {
                file.renameTo(new File(fileName + "_load_err_" + System.currentTimeMillis()));
                file.delete();
                this.store = MVStore.open(fileName);
            } else {
                throw err;
            }
        }
        init();
    }

    public MVStoreTimeBaseFileStore(MVStore store) {
        this.store = store;
        init();
    }

    private void init() {
        store.getMapNames().forEach(this::getOrCreateCache);
    }


    public T get(String name, String key, long time) {
        MVMap<String, Refs> map = cache.get(name);
        if (map == null) {
            return null;
        }
        Refs refs = map.get(key);
        if (refs != null) {
            Ref ref = refs.getRef(time);
            return ref == null ? null : (T) ref.value;
        }
        return null;
    }

    public void set(String name, String key, long time, T value) {
        Refs refs = getOrCreateCache(name).computeIfAbsent(key, ignore -> new Refs());
        refs.maxRef = maxStoreSizeEachKey;
        refs.update(new Ref(time, value));
    }

    @Override
    public void remove(String name, String key) {
        MVMap<String, Refs> map = cache.get(name);
        if (map == null) {
            return;
        }
        map.remove(key);
    }

    @Override
    public void removeAll(String name) {
        cache.remove(name);
        store.removeMap(name);
    }

    @Override
    public void dispose() {
        store.compactFile((int)Duration.ofSeconds(30).toMillis());
        store.close();
    }

    @Override
    public void clear() {
        store.getMapNames().forEach(store::removeMap);
        store.commit();
        store.compactFile((int)Duration.ofSeconds(30).toMillis());
    }

    protected MVMap<String, Refs> getOrCreateCache(String key) {
        return cache.computeIfAbsent(key, store::openMap);
    }


    public static class Refs implements Externalizable {
        private static final long serialVersionUID = 1;
        private Ref[] refs;
        private long minTime = -1;
        private int maxRef = DEFAULT_MAX_STORE_SIZE_EACH_KEY;

        public Ref getRef(long baseTime) {
            for (Ref ref : refs) {
                if (ref != null && ref.time <= baseTime) {
                    return ref;
                }
            }
            return null;
        }

        public void update(Ref ref) {
            if (refs == null) {
                refs = new Ref[0];
            }
            if (minTime > 0) {
                //时间回退?
                if (ref.time < minTime) {
                    return;
                }
            }

            boolean newEl = false;
            if (refs.length < maxRef) {
                refs = Arrays.copyOf(refs, refs.length + 1);
                newEl = true;
            }

            Ref last = refs[0];
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
        @SneakyThrows
        public void writeExternal(ObjectOutput out) {
            out.writeByte(refs == null ? 0 : refs.length);
            if (refs != null) {
                for (Ref ref : refs) {
                    ref.writeExternal(out);
                }
            }
        }

        @Override
        @SneakyThrows
        public void readExternal(ObjectInput in) {
            byte length = in.readByte();
            if (length != 0) {
                refs = new Ref[length];
            }
            for (byte i = 0; i < length; i++) {
                Ref ref = new Ref();
                ref.readExternal(in);
                refs[i] = ref;
            }
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    public static class Ref implements Externalizable {
        private static final long serialVersionUID = 1;
        private long time;
        private Object value;

        @Override
        @SneakyThrows
        public void writeExternal(ObjectOutput out) {
            out.writeLong(time);
            out.writeObject(value);
        }

        @Override
        @SneakyThrows
        public void readExternal(ObjectInput in) {
            time = in.readLong();
            value = in.readObject();
        }

        @Override
        public String toString() {
            return "Ref{" +
                    "time=" + time +
                    ", value=" + value +
                    '}';
        }
    }
}
