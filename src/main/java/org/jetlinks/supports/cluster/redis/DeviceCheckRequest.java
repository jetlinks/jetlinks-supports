package org.jetlinks.supports.cluster.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DeviceCheckRequest implements Serializable {

    private String from;

    private String requestId;

    private List<String> deviceId;


}
