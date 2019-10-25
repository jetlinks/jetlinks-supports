package org.jetlinks.supports.cluster.redis;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
@ToString
public class NotifierMessage implements Serializable {

    private String messageId;

    private String fromServer;

    private String address;

    private Object payload;


}
