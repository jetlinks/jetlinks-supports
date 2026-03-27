package org.jetlinks.supports.context;

import io.micrometer.context.ContextSnapshot;
import io.micrometer.context.ContextSnapshotFactory;
import reactor.util.context.ContextView;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 响应式上下文辅助工具,用于传递响应式上下文到阻塞操作中.
 */
public class ReactorContextHelper {

    static final ContextSnapshotFactory factory = ContextSnapshotFactory
        .builder()
        .clearMissing(true)
        .build();


    public static <T> Callable<T> wrapBlocking(ContextView context, Callable<T> callable) {
        return factory.captureFrom(context).wrap(callable);
    }

    public static <T> Consumer<T> wrapBlocking(ContextView context, Consumer<T> runnable) {
        return factory.captureFrom(context).wrap(runnable);
    }

    public static Runnable wrapBlocking(ContextView context, Runnable runnable) {
        return factory.captureFrom(context).wrap(runnable);
    }

    public static <T> Supplier<T> wrapBlocking(ContextView context, Supplier<T> supplier) {
        return () -> {
            try (ContextSnapshot.Scope scope = factory.setThreadLocalsFrom(context)) {
                return supplier.get();
            }
        };

    }

}
