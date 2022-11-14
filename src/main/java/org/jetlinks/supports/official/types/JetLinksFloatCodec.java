package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.FloatType;

@Getter
@Setter
public class JetLinksFloatCodec extends JetLinksNumberCodec<FloatType> {

    @Override
    public String getTypeId() {
        return FloatType.ID;
    }

}
