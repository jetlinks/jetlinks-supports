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
public class NotifierMessageReply implements Serializable {


    private String address;

    private String messageId;

    private Object payload;

    private boolean success;

    private String errorMessage;

    public static NotifierMessageReply success(String address, String messageId, Object payload) {
        return of(address, messageId, payload, true, null);
    }

    public static NotifierMessageReply fail(String address, String messageId, Throwable e) {
        return of(address, messageId, null, false, e.getClass().getName().concat(":").concat(e.getMessage()));
    }
}
