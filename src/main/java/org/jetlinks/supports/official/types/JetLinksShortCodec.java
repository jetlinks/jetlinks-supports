package org.jetlinks.supports.official.types;

import org.jetlinks.core.metadata.types.ShortType;

public class JetLinksShortCodec extends JetLinksNumberCodec<ShortType> {

    @Override
    public String getTypeId() {
        return ShortType.ID;
    }

}
