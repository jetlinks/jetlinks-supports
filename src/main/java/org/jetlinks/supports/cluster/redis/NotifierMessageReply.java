package org.jetlinks.supports.cluster.redis;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
@ToString
public class NotifierMessageReply implements Serializable {


    private String address;

    private String messageId;

    private Object payload;

    private boolean success;

    private String errorMessage;

    private boolean complete;

    public static NotifierMessageReply complete(String address, String messageId) {
        return of(address, messageId, null, true, null,true);
    }

    public static NotifierMessageReply success(String address, String messageId, Object payload) {
        return of(address, messageId, payload, true, null,false);
    }

    public static NotifierMessageReply fail(String address, String messageId, Throwable e) {
        return of(address, messageId, null, false, e.getClass().getName().concat(":").concat(String.valueOf(e.getMessage())),false);
    }
}
