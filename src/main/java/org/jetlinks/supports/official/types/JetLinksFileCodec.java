package org.jetlinks.supports.official.types;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.metadata.types.FileType;
import org.springframework.http.MediaType;

import java.util.Map;
import java.util.Optional;

@Getter
@Setter
@Deprecated
public class JetLinksFileCodec extends AbstractDataTypeCodec<FileType> {

    @Override
    public String getTypeId() {
        return FileType.ID;
    }


    @Override
    public FileType decode(FileType type, Map<String, Object> config) {
        super.decode(type, config);

        Optional.ofNullable(config.get("bodyType"))
                .map(String::valueOf)
                .flatMap(FileType.BodyType::of)
                .ifPresent(type::setBodyType);

        Optional.ofNullable(config.get("mediaType"))
                .map(String::valueOf)
                .map(MediaType::parseMediaType)
                .ifPresent(type::setMediaType);

        return type;

    }

    @Override
    protected void doEncode(Map<String, Object> encoded, FileType type) {
        super.doEncode(encoded, type);
        encoded.put("bodyType", type.getBodyType().name());
        encoded.put("mediaType", type.getMediaType().toString());
    }
}
