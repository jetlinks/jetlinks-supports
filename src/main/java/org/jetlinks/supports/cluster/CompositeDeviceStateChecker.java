package org.jetlinks.supports.cluster;

import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceStateChecker;
import reactor.core.publisher.Mono;

import javax.validation.constraints.NotNull;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class CompositeDeviceStateChecker implements DeviceStateChecker {

    private final List<DeviceStateChecker> checkerList = new CopyOnWriteArrayList<>();

    public void addDeviceStateChecker(DeviceStateChecker checker) {
        checkerList.add(checker);
        checkerList.sort(Comparator.comparing(DeviceStateChecker::order));
    }

    @Override
    public @NotNull Mono<Byte> checkState(@NotNull DeviceOperator device) {
        if(checkerList.isEmpty()){
            return Mono.empty();
        }
        if (checkerList.size() == 1) {
            return checkerList.get(0).checkState(device);
        }
        Mono<Byte> checker = checkerList.get(0).checkState(device);
        for (int i = 1, len = checkerList.size(); i < len; i++) {
            checker = checker.switchIfEmpty(checkerList.get(i).checkState(device));
        }
        return checker;
    }
}
