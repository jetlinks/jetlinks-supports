package org.jetlinks.supports.command;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import org.hswebframework.web.aop.MethodInterceptorHolder;
import org.hswebframework.web.i18n.LocaleUtils;
import org.jetlinks.core.annotation.Attr;
import org.jetlinks.core.command.*;
import org.jetlinks.core.metadata.*;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.utils.MetadataUtils;
import org.jetlinks.supports.official.DeviceMetadataParser;
import org.springframework.beans.MethodInvocationException;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 使用java类来实现命令
 *
 * @author zhouhao
 * @since 1.2.2
 */
public class JavaBeanCommandSupport extends AbstractCommandSupport {

    private static final Predicate<Method> defaultFilter
        = method -> {
        org.jetlinks.core.annotation.command.CommandHandler annotation = AnnotatedElementUtils
            .findMergedAnnotation(method, org.jetlinks.core.annotation.command.CommandHandler.class);

        return !Modifier.isStatic(method.getModifiers())
            && Modifier.isPublic(method.getModifiers())
            && method.getDeclaringClass() != Object.class
            //如果注解了则不能忽略
            && (annotation == null || !annotation.ignore());
    };

    protected final Object target;

    public JavaBeanCommandSupport(Object target, Collection<String> filter) {
        this(target, m -> filter.contains(m.getName()));
    }

    public JavaBeanCommandSupport(Object target, Predicate<Method> filter) {
        this.target = target;
        init(defaultFilter.and(filter));
    }

    public JavaBeanCommandSupport(Object target) {
        this(target, method -> AnnotatedElementUtils
            .findMergedAnnotation(method, org.jetlinks.core.annotation.command.CommandHandler.class) != null);
    }

    private void init(Predicate<Method> filter) {
        Class<?> clazz = ClassUtils.getUserClass(target);

        ReflectionUtils
            .doWithMethods(
                clazz,
                method -> {
                    if (filter.test(method)) {
                        register(method, clazz);
                    }
                });
    }

    private boolean returnIsVoid(ResolvableType type) {
        if (type.getGenerics().length > 0) {
            return returnIsVoid(type.getGeneric(0));
        }
        return type.toClass() == Void.class || type.toClass() == void.class;
    }

    @SneakyThrows
    private static Object doInvoke(Object target, Method method, Object... args) {
        try {
            return method.invoke(target, args);
        } catch (MethodInvocationException e) {
            if (e.getCause() != null) {
                throw e.getCause();
            }
            throw e;
        }
    }

    public List<CommandHandler<Command<?>, ?>> getHandlers() {
        return handlers
            .values()
            .stream()
            .distinct()
            .collect(Collectors.toList());
    }

    private void register(Method method, Class<?> owner) {
        Schema schema = AnnotationUtils.findAnnotation(method, Schema.class);
        Object target = this.target;
        String id = schema != null && StringUtils.hasText(schema.name()) ? schema.name() : method.getName();
        String name = id;
        String description = id;

        ResolvableType[] argTypes = new ResolvableType[method.getParameterCount()];
        String[] argNames = new String[method.getParameterCount()];
        List<PropertyMetadata> inputs = new ArrayList<>();
        DataType output = null;
        MethodInvoker invoker = null;

        for (int i = 0; i < method.getParameterCount(); i++) {
            argTypes[i] = ResolvableType.forMethodParameter(method, i, owner);
        }
        String[] discoveredArgName = MethodInterceptorHolder.nameDiscoverer.getParameterNames(method);
        Parameter[] parameters = method.getParameters();

        for (int i = 0; i < argNames.length; i++) {
            Parameter parameter = parameters[i];
            Schema schemaAnn = parameter.getAnnotation(Schema.class);
            if (schemaAnn != null && StringUtils.hasText(schemaAnn.name())) {
                argNames[i] = schemaAnn.name();
            } else {
                argNames[i] = (discoveredArgName != null && discoveredArgName.length > i) ? discoveredArgName[i] : "arg" + i;
            }
        }

        ResolvableType returnType = ResolvableType.forMethodReturnType(method, owner);
        if (!returnIsVoid(returnType)) {
            output = DeviceMetadataParser.withType(returnType);
        }
        if (argTypes.length == 0) {
            invoker = ignore -> doInvoke(target, method);
        } else {
            if (argTypes.length == 1) {
                //参数就是命令
                if (Command.class.isAssignableFrom(argTypes[0].toClass())) {
                    invoker = new CommandInvoker(target, method, argTypes[0]);
                    //不解析出参，以方法出参为准
                    FunctionMetadata resolve = CommandMetadataResolver.resolve(argTypes[0], ResolvableType.NONE);
                    id = resolve.getId();
                    name = resolve.getName();
                    description = resolve.getDescription();
                    inputs = resolve.getInputs();
                } else {
                    //转换为实体类
                    RequestBody requestBody = AnnotationUtils.findAnnotation(method.getParameters()[0], RequestBody.class);
                    if (requestBody != null) {
                        DataType metadataType = DeviceMetadataParser.withType(argTypes[0]);
                        if (metadataType instanceof ObjectType) {
                            inputs = ((ObjectType) metadataType).getProperties();
                        }
                        Type type = argTypes[0].toClass();
                        invoker = cmd -> {
                            Object param = cmd.as(type);
                            return doInvoke(target, method, param);
                        };
                    }
                }
            }
            if (invoker == null) {
                //多参数？
                for (int i = 0; i < argTypes.length; i++) {
                    ResolvableType type = argTypes[i];
                    Parameter parameter = parameters[i];
                    Schema schemaAnn = parameter.getAnnotation(Schema.class);
                    DataType dataType = DeviceMetadataParser.withType(type);
                    SimplePropertyMetadata metadata = new SimplePropertyMetadata();
                    metadata.setId(argNames[i]);

                    metadata.setDescription(schemaAnn == null ? null : schemaAnn.description());
                    metadata.setName(schemaAnn != null && StringUtils.hasText(schemaAnn.title())
                                         ? schemaAnn.title()
                                         : metadata.getId());
                    metadata.setValueType(dataType);
                    inputs.add(metadata);
                }
                invoker = cmd -> {
                    Object[] args = new Object[argTypes.length];
                    for (int i = 0; i < argTypes.length; i++) {
                        ResolvableType type = argTypes[i];
                        args[i] = cmd.getOrNull(argNames[i], type.toClass());
                    }
                    return doInvoke(target, method, args);
                };
            }
        }

        SimpleFunctionMetadata metadata = new SimpleFunctionMetadata();
        metadata.setId(id);
        metadata.setOutput(output);
        metadata.setInputs(inputs);
        metadata.setName(schema != null && StringUtils.hasText(schema.title()) ? schema.title() : name);
        metadata.setDescription(schema != null && StringUtils.hasText(schema.description()) ? schema.description() : description);

        MethodCallCommandHandler handler = new MethodCallCommandHandler(
            invoker,
            applyMetadata(method, argTypes, metadata),
            method);

        //优先注册子类重写的方法
        registerHandlerAbsent(metadata.getId(), handler);

    }

    @SuppressWarnings("all")
    protected <C extends Command<R>, R> void registerHandlerAbsent(String id,
                                                                   CommandHandler<C, R> handler) {
        handlers.computeIfAbsent(id, k -> (CommandHandler<Command<?>, ?>) handler);
    }

    protected FunctionMetadata applyMetadata(Method method,
                                             ResolvableType[] argTypes,
                                             SimpleFunctionMetadata metadata) {
        org.jetlinks.core.annotation.command.CommandHandler annotation = AnnotatedElementUtils
            .getMergedAnnotation(method, org.jetlinks.core.annotation.command.CommandHandler.class);
        if (annotation == null) {
            return metadata;
        }

        //自定义命令
        if (annotation.value() != Command.class) {
            //不解析出参，以方法出参为准
            FunctionMetadata resolve = CommandMetadataResolver.resolve(annotation.value(), Object.class);
            metadata.setId(resolve.getId());
            metadata.setName(resolve.getName());
            metadata.setDescription(resolve.getDescription());
            metadata.setInputs(resolve.getInputs());
        }
        //自定义了命令ID
        if (StringUtils.hasText(annotation.id())) {
            metadata.setId(annotation.id());
        }
        //命令名称
        if (StringUtils.hasText(annotation.name())) {
            metadata.setName(LocaleUtils.resolveMessage(annotation.name(), annotation.name()));
        }
        //命令描述
        if (StringUtils.hasText(annotation.description())) {
            metadata.setDescription(annotation.description());
        }

        metadata.setExpands(MetadataUtils.parseExpands(method));

        for (Attr expand : annotation.expands()) {
            metadata.expand(expand.key(), expand.value());
        }
        metadata.expand(CommandConstant.responseFlux,
                        Flux.class.isAssignableFrom(method.getReturnType()));

        //自定义输入参数描述
        if (!Void.class.equals(annotation.inputSpec())) {
            metadata.setInputs(CommandMetadataResolver.resolveInputs(ResolvableType.forType(annotation.inputSpec())));
        }
        if (!Void.class.equals(annotation.outputSpec())) {
            metadata.setOutput(CommandMetadataResolver.resolveOutput(ResolvableType.forType(annotation.outputSpec())));
        }
        return metadata;
    }

    interface MethodInvoker extends Function<Command<Object>, Object> {

    }

    @AllArgsConstructor
    static class CommandInvoker implements MethodInvoker {
        private final Object target;
        private final Method method;
        private final ResolvableType type;

        @Override
        public Object apply(Command<Object> objectCommand) {
            //类型相同直接调用
            if (type.toClass().isInstance(objectCommand)) {
                return doInvoke(target, method, objectCommand);
            } else {
                //copy类型
                return doInvoke(target, method, createCommand().with(objectCommand.asMap()));
            }
        }

        @SneakyThrows
        private Command<Object> createCommand() {
            @SuppressWarnings("all")
            Command<Object> cmd = (Command<Object>) type
                .toClass()
                .getDeclaredConstructor()
                .newInstance();
            return cmd;
        }

    }

    @AllArgsConstructor
    static class MethodCallCommandHandler implements CommandHandler<Command<Object>, Object> {
        private final MethodInvoker invoker;
        private final FunctionMetadata metadata;
        private final Method method;

        @Override
        public Object handle(@Nonnull Command<Object> command, @Nonnull CommandSupport support) {
            return invoker.apply(command);
        }

        @Nonnull
        @Override
        @SuppressWarnings("all")
        public Command<Object> createCommand() {
            if (metadata.getExpand(CommandConstant.responseFlux).orElse(false)) {
                return (Command) new MethodCallFluxCommand(metadata.getId());
            }
            return new MethodCallCommand(metadata.getId());
        }

        @Override
        public FunctionMetadata getMetadata() {
            return metadata;
        }

        @Override
        public String toString() {
            return String.valueOf(method);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MethodCallFluxCommand extends AbstractCommand<Flux<Object>, MethodCallFluxCommand> {
        private String commandId;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MethodCallCommand extends AbstractCommand<Object, MethodCallCommand> {
        private String commandId;
    }

    @Override
    public String toString() {
        return handlers.values().toString();
    }
}
