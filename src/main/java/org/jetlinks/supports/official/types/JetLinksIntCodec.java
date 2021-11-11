package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.IntType;

@Getter
@Setter
public class JetLinksIntCodec extends JetLinksNumberCodec<IntType> {

    @Override
    public String getTypeId() {
        return IntType.ID;
    }


}
