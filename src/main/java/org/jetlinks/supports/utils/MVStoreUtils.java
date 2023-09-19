package org.jetlinks.supports.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.MVStore;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            try {
                MVStore store = open0(customizer);
                fireEvent(Action.success);
                return store;
            } catch (Throwable e) {
                this.error = null;
                if (file.exists()) {
                    return tryRecovery(customizer);
                }
                throw e;
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

        private MVStore tryRecovery(Function<MVStore.Builder, MVStore.Builder> customizer) {
            try {
                log.warn("try recovery mvstore:{}", file);
                MVStore recover = open0(c -> customizer
                        .apply(c)
                        .recoveryMode());
                recover.compactFile(30_000);
                recover.close(30_000);
                log.warn("recovery mvstore:{} complete", file);
                return open0(customizer);
            } catch (Throwable err) {
                this.error = err;
                fireEvent(Action.recoverFail);
                try {
                    this.error = null;
                    backup = backup();
                    fireEvent(Action.backup);
                } catch (Throwable error) {
                    this.error = err;
                    fireEvent(Action.backupFail);
                }
                file.delete();
                return open0(customizer);
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
