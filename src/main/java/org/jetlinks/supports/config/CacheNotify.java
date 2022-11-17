package org.jetlinks.supports.config;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jetlinks.core.utils.SerializeUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class CacheNotify implements Externalizable {
    private static final long serialVersionUID = 1L;

    private String name;
    private Collection<String> keys;
    private boolean clear;

    public static CacheNotify clear(String name) {
        return CacheNotify.of(name, null, true);
    }

    public static CacheNotify expires(String name, Collection<String> keys) {
        return CacheNotify.of(name, keys, false);
    }

    public static CacheNotify expiresAll(String name) {
        return CacheNotify.of(name, null, false);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(clear);
        SerializeUtils.writeObject(keys, out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        clear = in.readBoolean();
        keys = (Collection<String>) SerializeUtils.readObject(in);
    }

    @Override
    public String toString() {
        return clear ? name + ":clear" : name + ":" + keys;
    }
}