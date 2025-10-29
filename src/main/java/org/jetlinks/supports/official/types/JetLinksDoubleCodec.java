package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.DoubleType;

@Getter
@Setter
@Deprecated
public class JetLinksDoubleCodec extends JetLinksNumberCodec<DoubleType> {

    @Override
    public String getTypeId() {
        return DoubleType.ID;
    }

}
