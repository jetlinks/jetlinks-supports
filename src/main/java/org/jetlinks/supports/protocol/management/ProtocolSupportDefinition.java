package org.jetlinks.supports.protocol.management;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ProtocolSupportDefinition implements Serializable {

    private static final long serialVersionUID = -1;

    private String id;
    private String name;
    private String description;

    private String provider;//jar script

    private byte state;

    private Map<String,Object> configuration;

}
