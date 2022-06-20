package things;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.things.ThingProperty;
import org.jetlinks.core.utils.SerializeUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@Getter
@Setter
public class SerializableThingProperty implements ThingProperty, Externalizable {
    private static final long serialVersionUID = 1;

    private String property;
    private long timestamp;
    private Object value;
    private String state;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(property);
        out.writeObject(value);
        out.writeLong(timestamp);
        SerializeUtils.writeNullableUTF(state, out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        property = in.readUTF();
        value = in.readObject();
        timestamp = in.readLong();
        state = SerializeUtils.readNullableUTF(in);
    }

}
