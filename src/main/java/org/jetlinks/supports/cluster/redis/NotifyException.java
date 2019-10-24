package org.jetlinks.supports.cluster.redis;


import lombok.Getter;

@Getter
public class NotifyException extends RuntimeException {

    private String address;


    public NotifyException(String address, String message) {
        super(message);
        this.address = address;
    }


}
