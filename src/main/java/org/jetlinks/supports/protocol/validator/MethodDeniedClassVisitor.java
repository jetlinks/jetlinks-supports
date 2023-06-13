package org.jetlinks.supports.protocol.validator;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.asm.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class MethodDeniedClassVisitor extends ClassVisitor {

    private final Set<String> methods = new HashSet<>();

    static final FastThreadLocal<String> clazzName = new FastThreadLocal<>();

    @SneakyThrows
    public void validate(String className, InputStream classStream) {
        clazzName.set(className);
        try {
            ClassReader classReader = new ClassReader(classStream);
            classReader.accept(this, 0);
        } finally {
            clazzName.set(null);
        }
    }


    public void addDefaultDenied() {
        addDenied(Flux.class, "blockFirst");
        addDenied(Flux.class, "blockLast");
        addDenied(Mono.class, "block");
        addDenied(Mono.class, "blockOptional");

        addDenied("reactor/core/publisher/MonoProcessor.block");
        addDenied("reactor/core/publisher/MonoProcessor.blockOptional");

        addDenied(CompletableFuture.class, "get");
        addDenied(System.class, "exit");
        addDenied(Runtime.class, "exit");
        addDenied(Runtime.class, "exec");
        addDenied(Runtime.class, "halt");

        addDenied(Thread.class, "sleep");
        addDenied(Thread.class, "interrupt");

    }

    public void addDenied(String method) {
        methods.add(method);
    }

    public void addDenied(Class<?> clazz, String method) {
        addDenied(clazz.getName().replace(".", "/") + "." + method);
    }

    public MethodDeniedClassVisitor() {
        super(Opcodes.ASM7);
    }


    @Override
    public MethodVisitor visitMethod(int access, String methodName, String descriptor, String signature, String[] exceptions) {

        return new MethodVisitor(api) {
            int line = -1;

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
                if (methods.contains(owner + "." + name) ||
                        methods.contains(owner + ".*")) {
                    throw new MethodInvokeDeniedException(
                            clazzName.get(),
                            methodName,
                            owner.replace("/", "."),
                            name,
                            line
                    );
                }
            }

            @Override
            public void visitLineNumber(int line, Label start) {
                super.visitLineNumber(line, start);
                this.line = line;
            }
        };
    }


}
