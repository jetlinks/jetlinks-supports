package org.jetlinks.supports.rpc;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
public class MethodRequest {
    private String method;
    private Object[] args;

}