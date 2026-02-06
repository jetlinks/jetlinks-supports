package org.jetlinks.supports.protocol.blocking;

import lombok.AllArgsConstructor;
import org.jetlinks.core.defaults.BlockingDeviceOperator;
import org.jetlinks.core.device.DevicePrincipal;
import org.jetlinks.core.principal.Credential;
import org.jetlinks.core.principal.Identity;

@AllArgsConstructor
public class BlockingDevicePrincipal implements DevicePrincipal {
    private final BlockingDeviceOperator device;
    private final DevicePrincipal principal;

    @Override
    public BlockingDeviceOperator getDevice() {
        return device;
    }

    @Override
    public boolean isVerified() {
        return principal.isVerified();
    }

    @Override
    public Identity identity() {
        return principal.identity();
    }


    @Override
    public Credential credential() {
        return principal.credential();
    }
}
