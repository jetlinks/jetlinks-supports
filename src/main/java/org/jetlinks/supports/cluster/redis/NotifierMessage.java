package org.jetlinks.supports.cluster.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class NotifierMessage implements Serializable {

    private String messageId;

    private String fromServer;

    private String address;

    private Object payload;


}
