package org.jetlinks.supports.command;

import org.jetlinks.core.command.AbstractCommandSupport;
import org.jetlinks.core.command.CommandHandler;

public abstract class AnnotationCommandSupport extends AbstractCommandSupport {


    public AnnotationCommandSupport(){
        registerCommands(this);
    }

    @SuppressWarnings("all")
    protected void registerCommands(Object instance) {
        new JavaBeanCommandSupport(instance)
            .getHandlers()
            .forEach(handler -> registerHandler(handler.getMetadata().getId(), (CommandHandler) handler));
    }
}
