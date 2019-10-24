package org.jetlinks.supports.cluster.redis;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jetlinks.core.device.DeviceStateInfo;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DeviceCheckResponse implements Serializable {

    private List<DeviceStateInfo> stateInfoList;

    private String requestId;

}
