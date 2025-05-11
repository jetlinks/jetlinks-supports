package org.jetlinks.supports.command;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.web.aop.MethodInterceptorHolder;
import org.hswebframework.web.i18n.LocaleUtils;
import org.jetlinks.core.command.*;
import org.jetlinks.core.metadata.*;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.utils.MetadataUtils;
import org.jetlinks.supports.official.DeviceMetadataParser;
import org.reactivestreams.Publisher;
import org.springframework.beans.MethodInvocationException;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.function.Function3;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 使用java类来实现命令
 *
 * @author zhouhao
 * @since 1.2.2
 */
@Slf4j
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
    protected final ResolvableType targetType;

    public JavaBeanCommandSupport(Object target, Collection<String> filter) {
        this(target, m -> filter.contains(m.getName()));
    }

    public JavaBeanCommandSupport(Object target, Predicate<Method> filter) {
        this.target = target;
        this.targetType = ResolvableType.forInstance(target);
        init(defaultFilter.and(filter));
    }

    public JavaBeanCommandSupport(Object target) {
        this(target, method -> AnnotatedElementUtils
            .findMergedAnnotation(method, org.jetlinks.core.annotation.command.CommandHandler.class) != null);
    }

    JavaBeanCommandSupport(ResolvableType targetType, Collection<String> filter) {
        this(targetType, m -> filter.contains(m.getName()));
    }

    JavaBeanCommandSupport(ResolvableType targetType, Predicate<Method> filter) {
        this.target = null;
        this.targetType = targetType;
        init(defaultFilter.and(filter));
    }

    JavaBeanCommandSupport(ResolvableType targetType) {
        this(targetType, method -> AnnotatedElementUtils
            .findMergedAnnotation(method, org.jetlinks.core.annotation.command.CommandHandler.class) != null);
    }

    public static JavaBeanCommandSupport createTemplate(ResolvableType type) {
        return new JavaBeanCommandSupport(type);
    }

    private void init(Predicate<Method> filter) {
        ReflectionUtils
            .doWithMethods(
                targetType.toClass(),
                method -> {
                    if (filter.test(method)) {
                        register(method);
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

    private static class MethodDesc {
        final ResolvableType[] argTypes;
        final String[] argNames;
        final Method method;
        final ResolvableType returnType;
        final Parameter[] parameters;

        public MethodDesc(ResolvableType owner, Method method) {
            this.method = method;
            argTypes = new ResolvableType[method.getParameterCount()];
            argNames = new String[method.getParameterCount()];
            parameters = method.getParameters();
            for (int i = 0; i < method.getParameterCount(); i++) {
                argTypes[i] = ResolvableType.forMethodParameter(new MethodParameter(method,i), owner);
            }
            String[] discoveredArgName = MethodInterceptorHolder.nameDiscoverer.getParameterNames(method);

            for (int i = 0; i < argNames.length; i++) {
                Parameter parameter = parameters[i];
                Schema schemaAnn = parameter.getAnnotation(Schema.class);
                if (schemaAnn != null && StringUtils.hasText(schemaAnn.name())) {
                    argNames[i] = schemaAnn.name();
                } else {
                    argNames[i] = (discoveredArgName != null && discoveredArgName.length > i) ? discoveredArgName[i] : "arg" + i;
                }
            }
            returnType = ResolvableType.forMethodParameter(new MethodParameter(method,-1), owner);
        }

        public MethodInvoker createMethodInvoker() {
            ResolvableType[] argTypes = this.argTypes;
            String[] argNames = this.argNames;
            Method method = this.method;
            ReflectionUtils.makeAccessible(method);
            if (argTypes.length == 0) {
                return (target, ignore) -> doInvoke(target, method);
            }
            if (argTypes.length == 1) {
                if (Command.class.isAssignableFrom(argTypes[0].toClass())) {
                    return new CommandInvoker(method, argTypes[0]);
                }
            }
            //转换为实体类
            RequestBody requestBody = AnnotationUtils.findAnnotation(parameters[0], RequestBody.class);
            if (requestBody != null) {
                Type type = argTypes[0].toClass();
                return (target, cmd) -> {
                    Object param = cmd.as(type);
                    return doInvoke(target, method, param);
                };
            }

            return (target, cmd) -> {
                Object[] args = new Object[argTypes.length];
                for (int i = 0; i < argTypes.length; i++) {
                    ResolvableType type = argTypes[i];
                    args[i] = cmd.getOrNull(argNames[i], type.getType());
                }
                return doInvoke(target, method, args);
            };
        }

    }

    private void register(Method method) {
        ResolvableType owner = targetType;
        Schema schema = AnnotationUtils.findAnnotation(method, Schema.class);
        String id = schema != null && StringUtils.hasText(schema.name()) ? schema.name() : method.getName();
        String name = id;
        String description = id;

        MethodDesc desc = new MethodDesc(owner, method);
        ResolvableType returnType = ResolvableType.forMethodParameter(new MethodParameter(method,-1), owner);
        DataType output = null;
        MethodInvoker invoker = null;
        Parameter[] parameters = desc.parameters;
        String[] argNames = desc.argNames;
        List<PropertyMetadata> inputs = new ArrayList<>();
        ResolvableType[] argTypes = desc.argTypes;
        if (!returnIsVoid(returnType)) {
            output = DeviceMetadataParser.withType(returnType);
        }
        if (argTypes.length == 0) {
            invoker = (_target, ignore) -> doInvoke(_target, method);
        } else {
            if (argTypes.length == 1) {
                //参数就是命令
                if (Command.class.isAssignableFrom(argTypes[0].toClass())) {
                    invoker = new CommandInvoker(method, argTypes[0]);
                    //不解析出参，以方法出参为准
                    FunctionMetadata resolve = CommandMetadataResolver.resolve(desc.argTypes[0], ResolvableType.NONE);
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
                        invoker = (_target, cmd) -> {
                            Object param = cmd.as(type);
                            return doInvoke(_target, method, param);
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
                    MetadataUtils
                        .parseExpands(parameter.getAnnotations())
                        .forEach(metadata::expand);
                    inputs.add(metadata);
                }
                invoker = (_target, cmd) -> {
                    Object[] args = new Object[argTypes.length];
                    for (int i = 0; i < argTypes.length; i++) {
                        ResolvableType type = argTypes[i];
                        args[i] = cmd.getOrNull(argNames[i], type.getType());
                    }
                    return doInvoke(_target, method, args);
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
            this.target,
            invoker,
            applyMetadata(method, argTypes, metadata),
            createMetadataHandler(owner, method),
            method);

        //优先注册子类重写的方法
        registerHandlerAbsent(metadata.getId(), handler);

    }

    @SuppressWarnings("all")
    protected <C extends Command<R>, R> void registerHandlerAbsent(String id,
                                                                   CommandHandler<C, R> handler) {
        handlers.computeIfAbsent(id, k -> (CommandHandler<Command<?>, ?>) handler);
    }

    private static final MetadataHandler DO_NOTHING_HANDLER =
        (target, cmd, metadata) -> Mono.just(metadata);

    private MetadataHandler createMetadataHandler(ResolvableType owner, Method method) {

        org.jetlinks.core.annotation.command.CommandHandler annotation = AnnotatedElementUtils
            .getMergedAnnotation(method, org.jetlinks.core.annotation.command.CommandHandler.class);
        if (annotation == null || !StringUtils.hasText(annotation.outputProvider())) {
            return DO_NOTHING_HANDLER;
        }
        String provider = annotation.outputProvider();
        Method providerMethod = ReflectionUtils.findMethod(
            targetType.toClass(),
            provider,
            (Class<?>[]) null);
        if (providerMethod == null) {
            log.warn("outputProvider method [{}] not found for {}", provider, providerMethod);
            return DO_NOTHING_HANDLER;
        }
        MethodDesc desc = new MethodDesc(owner, providerMethod);
        ResolvableType returnType = desc.returnType;

        if (!DataType.class.isAssignableFrom(CommandUtils.getCommandResponseDataType(returnType).toClass())) {
            log.warn("outputProvider method [{}] return type not DataType for {}", provider, method);
            return DO_NOTHING_HANDLER;
        }

        return new DefaultMetadataHandler(desc.createMethodInvoker());
    }

    @AllArgsConstructor
    private static class DefaultMetadataHandler implements MetadataHandler {
        private final MethodInvoker invoker;

        private Mono<FunctionMetadata> convertMetadata(Object data, SimpleFunctionMetadata metadata) {
            if (data instanceof DataType) {
                metadata = metadata.copy();
                metadata.setOutput((DataType) data);
            }
            return Mono.just(metadata);
        }

        @Override
        public Mono<FunctionMetadata> apply(Object target, Command<?> objectCommand, SimpleFunctionMetadata metadata) {
            @SuppressWarnings("all")
            Object object = invoker.apply(target, (Command<Object>) objectCommand);
            if (object instanceof Publisher) {
                return Mono.from(((Publisher<?>) object))
                           .flatMap(data -> convertMetadata(data, metadata));
            }
            return convertMetadata(object, metadata);
        }
    }

    protected SimpleFunctionMetadata applyMetadata(Method method,
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

        MetadataUtils.resolveAttrs(annotation.expands(), metadata::expand);

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

    interface MethodInvoker extends BiFunction<Object, Command<Object>, Object> {

    }

    interface MetadataHandler extends Function3<Object, Command<?>, SimpleFunctionMetadata, Mono<FunctionMetadata>> {
        @Override
        Mono<FunctionMetadata> apply(Object target, Command<?> objectCommand, SimpleFunctionMetadata metadata);
    }

    @AllArgsConstructor
    static class CommandInvoker implements MethodInvoker {
        private final Method method;
        private final ResolvableType type;

        @Override
        public Object apply(Object target, Command<Object> objectCommand) {
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

    /**
     * 将当前命令支持作为模版，使用另外一个实现类来执行命令调用。
     *
     * @param target 实现类实例
     * @return CommandSupport
     */
    public CommandSupport copyWith(Object target) {
        return new CopyJavaBeanCommandSupport(this, target);
    }

    static class CopyJavaBeanCommandSupport extends TemplateCommandSupport {
        private final Object target;

        public CopyJavaBeanCommandSupport(JavaBeanCommandSupport parent, Object target) {
            super(parent);
            this.target = target;
        }

        @Nonnull
        @Override
        @SuppressWarnings("all")
        public <R> R execute(@Nonnull Command<R> command) {
            JavaBeanCommandSupport parent = template.unwrap(JavaBeanCommandSupport.class);
            //从注册的执行器中获取处理器进行执行
            CommandHandler handler = parent.handlers.get(command.getCommandId());
            if (handler instanceof MethodCallCommandHandler) {
                handler = ((MethodCallCommandHandler) handler).with(target);
            }
            if (handler == null) {
                return parent.executeUndefinedCommand(command);
            }
            return (R) handler.handle(command, parent);
        }
    }

    @AllArgsConstructor
    static class MethodCallCommandHandler implements CommandHandler<Command<Object>, Object> {
        private final Object target;
        private final MethodInvoker invoker;
        private final SimpleFunctionMetadata metadata;
        private final MetadataHandler metadataHandler;
        private final Method method;

        MethodCallCommandHandler with(Object target) {
            return new MethodCallCommandHandler(target, invoker, metadata, metadataHandler, method);
        }

        @Override
        public Object handle(@Nonnull Command<Object> command,
                             @Nonnull CommandSupport support) {
            if (target == null) {
                throw new UnsupportedOperationException("unsupported call not implement method " + method);
            }
            return invoker.apply(target, command);
        }

        @Override
        public Mono<FunctionMetadata> getMetadata(Command<?> cmd) {
            return metadataHandler.apply(target, cmd, metadata);
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
