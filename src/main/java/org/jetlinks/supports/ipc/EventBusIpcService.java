package org.jetlinks.supports.ipc;

import lombok.AllArgsConstructor;
import org.jetlinks.core.event.EventBus;
import org.jetlinks.core.ipc.IpcDefinition;
import org.jetlinks.core.ipc.IpcInvoker;
import org.jetlinks.core.ipc.IpcService;
import reactor.core.Disposable;

@AllArgsConstructor
public class EventBusIpcService implements IpcService {

    private final int id;

    private final EventBus eventBus;

    @Override
    public <REQ, RES> Disposable listen(IpcDefinition<REQ, RES> definition, IpcInvoker<REQ, RES> invoker) {
        return new EventBusIpcResponder<>(eventBus, definition, invoker);
    }

    @Override
    public <REQ, RES> IpcInvoker<REQ, RES> createInvoker(String name, IpcDefinition<REQ, RES> definition) {

        return new EventBusIpcRequester<>(id, name, eventBus, definition);
    }


}
