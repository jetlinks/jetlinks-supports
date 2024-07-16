package org.jetlinks.supports.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.*;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class MVStoreUtils {

    private static final List<MVStoreOpening.Hook> hooks = new CopyOnWriteArrayList<>();

    public static void addHook(MVStoreOpening.Hook hook) {
        hooks.add(hook);
    }

    public static MVStore open(File file,
                               String operationName,
                               Function<MVStore.Builder, MVStore.Builder> customizer) {
        return new MVStoreOpeningImpl(file, operationName, customizer).open();
    }

    public static <T> T open(File file,
                             String operationName,
                             Function<MVStore.Builder, MVStore.Builder> customizer,
                             Function<MVStore, T> handler) {
        return new MVStoreOpeningImpl(file, operationName, customizer).open(handler);
    }


    public static <K, V> MVMap<K, V> openMap(MVStore store,
                                             String name,
                                             MVMap.MapBuilder<MVMap<K, V>, K, V> builder) {
        try {
            return store.openMap(name, builder);
        } catch (Throwable error) {
            store.removeMap(name);
            log.warn("Open MVStore Map[{}] error,Maybe the file [{}] is broken?",
                     name,
                     store.getFileStore().getFileName(),
                     error);
            return store.openMap(name, builder);
        }
    }

    public interface MVStoreOpening {
        File getFile();

        String getOperation();

        File getBackup();

        Throwable getError();

        interface Hook {

            void on(Action action, MVStoreOpening opening);

        }

        enum Action {
            success,
            recoverFail,
            backup,
            backupFail
        }
    }

    @Getter
    @RequiredArgsConstructor
    private static class MVStoreOpeningImpl implements MVStoreOpening {
        private final File file;
        private final String operation;
        private final Function<MVStore.Builder, MVStore.Builder> customizer;
        private File backup;
        private Throwable error;

        private void fireEvent(Action action) {
            if (!hooks.isEmpty()) {
                for (Hook hook : hooks) {
                    hook.on(action, this);
                }
            }
        }

        public MVStore open() {
            return open(Function.identity());
        }

        public <T> T open(Function<MVStore, T> handler) {
            if (!file.getParentFile().exists()) {
                boolean ignore = file.getParentFile().mkdirs();
            }
            T res;
            try {
                MVStore store = open0(customizer);
                //执行自定义操作,如果报错也尝试恢复文件.
                res = handler.apply(store);
                fireEvent(Action.success);
                return res;
            } catch (Throwable e) {
                this.error = null;
                if (file.exists()) {
                    return tryRecovery(customizer, e, handler);
                } else {
                    throw e;
                }
            }
        }

        @SuppressWarnings("all")
        private File backup() {
            File backup = new File(file.getParentFile(), file.getName() + ".backup." + LocalDateTime
                .now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")));

            file.renameTo(backup);

            return backup;
        }

        private <T> T tryRecovery(Function<MVStore.Builder, MVStore.Builder> customizer,
                                  Throwable reason,
                                  Function<MVStore, T> handler) {
            try {
                log.warn("try recovery mvstore:{}", file);
                MVStoreTool.compactCleanUp(file.getAbsolutePath());
                MVStoreTool.compact(file.getAbsolutePath(), false);
                log.warn("recovery mvstore:{} complete", file);
                return handler.apply(open0(customizer));
            } catch (Throwable err) {
                err.addSuppressed(reason);
                this.error = err;
                fireEvent(Action.recoverFail);
                try {
                    this.error = null;
                    backup = backup();
                    fireEvent(Action.backup);
                } catch (Throwable error) {
                    error.addSuppressed(err);
                    this.error = err;
                    fireEvent(Action.backupFail);
                }
                boolean ignore = file.delete();
                return handler.apply(open0(customizer));
            }

        }


        private MVStore open0(Function<MVStore.Builder, MVStore.Builder> customizer) {
            MVStore.Builder builder = new MVStore.Builder()
                .fileName(file.getAbsolutePath())
                //64MB
                .autoCommitBufferSize(64 * 1024)
                .compress()
                .keysPerPage(1024)
                .cacheSize(64);
            builder = customizer.apply(builder);
            return builder.open();
        }
    }

}
