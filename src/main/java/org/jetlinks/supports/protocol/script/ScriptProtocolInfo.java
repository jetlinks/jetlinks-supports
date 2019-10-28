package org.jetlinks.supports.protocol.script;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class ScriptProtocolInfo implements Serializable {

    private String id;
    private String name;
    private String description;

    private String transport;

    private String lang;

    private String authenticator;

    private String messageEncoder;

    private String messageDecoder;



}
