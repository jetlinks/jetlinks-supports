package org.jetlinks.supports.official.types;

import org.jetlinks.core.metadata.types.ByteType;

@Deprecated
public class JetLinksByteCodec extends JetLinksNumberCodec<ByteType> {

    public JetLinksByteCodec() {
    }

    @Override
    public String getTypeId() {
        return ByteType.ID;
    }

}
