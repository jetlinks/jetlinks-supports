package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.LongType;

@Getter
@Setter
public class JetLinksLongCodec extends JetLinksNumberCodec<LongType> {

    @Override
    public String getTypeId() {
        return LongType.ID;
    }

}
