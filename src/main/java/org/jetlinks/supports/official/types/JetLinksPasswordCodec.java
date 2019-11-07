package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.PasswordType;

@Getter
@Setter
public class JetLinksPasswordCodec extends AbstractDataTypeCodec<PasswordType> {

    @Override
    public String getTypeId() {
        return PasswordType.ID;
    }

}
