package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.GeoShapeType;

@Getter
@Setter
public class JetLinksGeoShapeCodec extends AbstractDataTypeCodec<GeoShapeType> {

    @Override
    public String getTypeId() {
        return GeoShapeType.ID;
    }

}
