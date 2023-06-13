package org.jetlinks.supports.protocol.validator;

import io.netty.util.concurrent.FastThreadLocal;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.asm.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class MethodDeniedClassVisitor extends ClassVisitor {

    private final Set<String> denied = new HashSet<>();

    static final FastThreadLocal<String> clazzName = new FastThreadLocal<>();

    private final static MethodDeniedClassVisitor GLOBAL = new MethodDeniedClassVisitor();

    static {
        GLOBAL.addDenied(MethodDeniedClassVisitor.class, "*");

        GLOBAL.addDenied(Flux.class, "blockFirst");
        GLOBAL.addDenied(Flux.class, "blockLast");
        GLOBAL.addDenied(Flux.class, "toIterable");
        GLOBAL.addDenied(Flux.class, "toStream");

        GLOBAL.addDenied(Mono.class, "block");
        GLOBAL.addDenied(Mono.class, "blockOptional");
        GLOBAL.addDenied(Mono.class, "toFuture");

        GLOBAL.addDenied("reactor.core.publisher.MonoProcessor.block");
        GLOBAL.addDenied("reactor.core.publisher.MonoProcessor.blockOptional");

        GLOBAL.addDenied(System.class, "exit");
        GLOBAL.addDenied(Runtime.class, "exit");
        GLOBAL.addDenied(Runtime.class, "exec");
        GLOBAL.addDenied(Runtime.class, "halt");
    }

    public static MethodDeniedClassVisitor global() {
        return GLOBAL;
    }


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
        denied.addAll(GLOBAL.denied);
    }

    public void removeDenied(String method) {
        denied.remove(method);
    }

    public void addDenied(String method) {
        denied.add(method);
    }

    public void removeDenied(Class<?> clazz, String method) {
        removeDenied(clazz.getName() + "." + method);
    }

    public void addDenied(Class<?> clazz, String method) {
        addDenied(clazz.getName() + "." + method);
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
                owner = owner.replace("/", ".");
                if (denied.contains(owner + "." + name) ||
                        denied.contains(owner + ".*")) {
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
