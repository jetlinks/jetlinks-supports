package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.StringType;

@Getter
@Setter
public class JetLinksStringCodec extends AbstractDataTypeCodec<StringType> {

    @Override
    public String getTypeId() {
        return StringType.ID;
    }

}
