package org.jetlinks.supports.command;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.hswebframework.web.aop.MethodInterceptorHolder;
import org.hswebframework.web.i18n.LocaleUtils;
import org.jetlinks.core.Wrapper;
import org.jetlinks.core.command.*;
import org.jetlinks.core.lang.SeparatedCharSequence;
import org.jetlinks.core.lang.SharedPathString;
import org.jetlinks.core.metadata.*;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.core.trace.FluxTracer;
import org.jetlinks.core.trace.MonoTracer;
import org.jetlinks.core.trace.TraceHolder;
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
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 使用java类来实现命令
 * <p>
 * 场景1. 使用一个javabean来创建命令支持.
 * <pre>{@code
 *
 *    class MyCommandApiImpl{
 *        @CommandHandler
 *        public Mono<Integer> reduce(int l,int r){
 *            return Mono.just(l+r);
 *        }
 *    }
 *
 *   static final JavasBeanCommandSupport support = new JavaBeanCommandSupport(new MyCommandApiImpl());
 *
 *   // 通过命令的方式调用
 *   public Mono<Object> callCommand(){
 *       return support
 *          .executeToMono("reduce", Map.of("l", 1, "r", 2));
 *   }
 * }</pre>
 *
 * <p>
 * 场景2. 使用接口来定义命令支持模版,用于在可能需要频繁创建命令支持时使用.
 * <pre>{@code
 *
 *  static final JavaBeanCommandSupport template =
 *              JavaBeanCommandSupport.createTemplate(MyCommandApi.class);
 *
 *  public CommandSupport createCommand(){
 *       // 创建实现类
 *       MyCommandApiImpl impl = new MyCommandApi();
 *       // 基于实现类创建命令支持
 *       return template.copyWith(impl);
 *  }
 * }</pre>
 *
 * @author zhouhao
 * @see JavaBeanCommandSupport#createTemplate(Class)
 * @see JavaBeanCommandSupport#copyWith(Object)
 * @see JavaBeanCommandSupport#copyHandlerWith(Object)
 * @since 1.2.2
 */
@Slf4j
public class JavaBeanCommandSupport extends AbstractCommandSupport {

    /**
     * 基于一个实例来创建一个命令支持对象,需要在方法上注解{@link org.jetlinks.core.annotation.command.CommandHandler}
     * 来声明命令处理器。
     *
     * @param instance 实例
     * @return JavaBeanCommandSupport
     */
    public static JavaBeanCommandSupport create(Object instance) {
        return new JavaBeanCommandSupport(instance);
    }

    /**
     * 使用指定的类型来创建一个命令支持模版,请缓存创建的模版对象,后续可使用{@link JavaBeanCommandSupport#copyWith(Object)}来创建执行实例.
     *
     * @param type 类型
     * @return JavaBeanCommandSupport
     * @see JavaBeanCommandSupport#copyWith(Object)
     * @see JavaBeanCommandSupport#copyHandlerWith(Object)
     */
    public static JavaBeanCommandSupport createTemplate(Class<?> type) {
        return createTemplate(ResolvableType.forType(type));
    }

    /**
     * 使用指定的类型来创建一个命令支持模版,请缓存创建的模版对象,后续可使用{@link JavaBeanCommandSupport#copyWith(Object)}来创建执行实例.
     *
     * @param type 类型
     * @return JavaBeanCommandSupport
     * @see JavaBeanCommandSupport#copyWith(Object)
     * @see JavaBeanCommandSupport#copyHandlerWith(Object)
     */
    public static JavaBeanCommandSupport createTemplate(ResolvableType type) {
        return new JavaBeanCommandSupport(type);
    }


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
        this.targetType = ResolvableType.forType(ClassUtils.getUserClass(target));
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
                argTypes[i] = ResolvableType.forMethodParameter(new MethodParameter(method, i), owner);
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
            returnType = ResolvableType.forMethodParameter(new MethodParameter(method, -1), owner);
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
        ResolvableType returnType = ResolvableType.forMethodParameter(new MethodParameter(method, -1), owner);
        DataType output = null;
        MethodInvoker invoker = null;
        Parameter[] parameters = desc.parameters;
        String[] argNames = desc.argNames;
        Map<String, Object> expands = null;
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
                    expands = resolve.getExpands();
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
        if (expands != null) {
            expands.forEach(metadata::expand);
        }
        MethodCallCommandHandler handler = new MethodCallCommandHandler(
            this.target,
            wrapInvoker(method, invoker),
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
            log.warn("outputProvider method [{}] not found", provider);
            return DO_NOTHING_HANDLER;
        }
        MethodDesc desc = new MethodDesc(owner, providerMethod);
        ResolvableType returnType = desc.returnType;

        if (!DataType.class.isAssignableFrom(CommandUtils.getCommandResponseDataType(returnType).toClass()) &&
            !FunctionMetadata.class.isAssignableFrom(CommandUtils.getCommandResponseDataType(returnType).toClass())) {
            log.warn("outputProvider method [{}] return type not DataType or FunctionMetadata for {}", provider, method);
            return DO_NOTHING_HANDLER;
        }

        return new DefaultMetadataHandler(desc.createMethodInvoker());
    }

    @AllArgsConstructor
    private static class DefaultMetadataHandler implements MetadataHandler {
        private final MethodInvoker invoker;

        private Mono<FunctionMetadata> convertMetadata(Object data, SimpleFunctionMetadata metadata) {
            if(data instanceof FunctionMetadata){
                return Mono.just((FunctionMetadata)data);
            }

            if (data instanceof DataType) {
                metadata = metadata.copy();
                metadata.setOutput((DataType) data);
            }

            return Mono.just(metadata);
        }

        @Override
        public Mono<FunctionMetadata> apply(Object target, Command<?> objectCommand, SimpleFunctionMetadata metadata) {
           return Mono.defer(()->{
               @SuppressWarnings("all")
               Object object = invoker.apply(target, (Command<Object>) objectCommand);
               if (object instanceof Publisher) {
                   return Mono.from(((Publisher<?>) object))
                              .flatMap(data -> convertMetadata(data, metadata));
               }
               return convertMetadata(object, metadata);
              })
               .as(LocaleUtils::transform);
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

        MetadataUtils
            .parseExpands(method)
            .forEach(metadata::expand);

        MetadataUtils.resolveAttrs(annotation.expands(), metadata::expand);

        if (!metadata.getExpand(CommandConstant.responseFlux).isPresent()) {
            metadata.expand(CommandConstant.responseFlux,
                            Flux.class.isAssignableFrom(method.getReturnType()));
        }
        //自定义输入参数描述
        if (!Void.class.equals(annotation.inputSpec())) {
            metadata.setInputs(CommandMetadataResolver.resolveInputs(ResolvableType.forType(annotation.inputSpec())));
        }
        if (!Void.class.equals(annotation.outputSpec())) {
            metadata.setOutput(CommandMetadataResolver.resolveOutput(ResolvableType.forType(annotation.outputSpec())));
        }
        return metadata;
    }

    protected interface MethodInvoker extends BiFunction<Object, Command<Object>, Object>, Wrapper {

    }

    interface MetadataHandler extends Function3<Object, Command<?>, SimpleFunctionMetadata, Mono<FunctionMetadata>> {
        @Override
        Mono<FunctionMetadata> apply(Object target, Command<?> objectCommand, SimpleFunctionMetadata metadata);
    }

    static class CommandInvoker implements MethodInvoker, Supplier<Command<Object>> {
        private final Method method;
        private final ResolvableType type;

        public CommandInvoker(Method method, ResolvableType type) {
            this.method = method;
            this.type = type;
        }

        @Override
        public Object apply(Object target, Command<Object> objectCommand) {
            //类型相同直接调用
            if (type.toClass().isInstance(objectCommand)) {
                return doInvoke(target, method, objectCommand);
            } else {
                //copy类型
                return doInvoke(target, method, createCommand().with(objectCommand));
            }
        }

        @Override
        public Command<Object> get() {
            return createCommand();
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

    /**
     * 基于新的实现类来复制命令处理器
     *
     * @param target 实现类实例
     * @return 命令处理器列表
     */
    public List<CommandHandler<?, ?>> copyHandlerWith(Object target) {
        return handlers
            .values()
            .stream()
            .filter(MethodCallCommandHandler.class::isInstance)
            .map(h -> MethodCallCommandHandler.class.cast(h).copy(target))
            .collect(Collectors.toList());

    }

    protected MethodInvoker wrapInvoker(Method method, MethodInvoker invoker) {
        return new TraceMethodInvoker(method, invoker);
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
                return (R) ((MethodCallCommandHandler) handler)
                    .handle0(target, (Command) command, parent);
            }
            if (handler == null) {
                return parent.executeUndefinedCommand(command);
            }
            return (R) handler.handle(command, parent);
        }

        @Override
        public Mono<FunctionMetadata> getCommandMetadata(@Nonnull String commandId,
                                                         @Nullable Map<String, Object> parameters) {
            if (MapUtils.isEmpty(parameters)) {
                return getCommandMetadata(commandId);
            }
            return this
                .createCommandAsync(commandId)
                .flatMap(cmd -> getCommandMetadata(cmd.with(parameters)));
        }

        @Override
        public Mono<FunctionMetadata> getCommandMetadata(Command<?> command) {
            JavaBeanCommandSupport parent = template.unwrap(JavaBeanCommandSupport.class);
            //从注册的执行器中获取处理器进行执行
            CommandHandler<?, ?> handler = parent.handlers.get(command.getCommandId());
            if (handler.isWrapperFor(MethodCallCommandHandler.class)) {
                return handler
                    .unwrap(MethodCallCommandHandler.class)
                    .getMetadata0(target, command);
            }
            return super.getCommandMetadata(command);
        }
    }

    protected static class TraceMethodInvoker implements MethodInvoker {
        private final static SharedPathString TRACE_TEMPLATE = SharedPathString.of("/java/command");
        private final Method method;
        private final MethodInvoker invoker;

        TraceMethodInvoker(Method method, MethodInvoker invoker) {
            this.method = method;
            this.invoker = invoker;
        }

        protected SeparatedCharSequence spanPrefix() {
            return TRACE_TEMPLATE;
        }

        @Override
        public Object apply(Object o, Command<Object> objectCommand) {
            if (TraceHolder.isDisabled()) {
                return invoker.apply(o, objectCommand);
            }
            SeparatedCharSequence span = spanPrefix()
                .append(ClassUtils.getUserClass(o).getSimpleName(), method.getName());
            if (Mono.class.isAssignableFrom(method.getReturnType())) {
                return Mono.defer(() -> (Mono<?>) invoker.apply(o, objectCommand))
                           .as(MonoTracer.create(span));
            }
            if (Flux.class.isAssignableFrom(method.getReturnType())) {
                return Flux.defer(() -> (Flux<?>) invoker.apply(o, objectCommand))
                           .as(FluxTracer.create(span));
            }
            return TraceHolder.traceBlocking(span, (ignore) -> invoker.apply(o, objectCommand));
        }

        @Override
        public boolean isWrapperFor(Class<?> type) {
            return invoker.isWrapperFor(type);
        }

        @Override
        public <T> T unwrap(Class<T> type) {
            return invoker.unwrap(type);
        }
    }

    @AllArgsConstructor
    static class MethodCallCommandHandler implements CommandHandler<Command<Object>, Object> {
        private final Object target;
        private final MethodInvoker invoker;
        private final SimpleFunctionMetadata metadata;
        private final MetadataHandler metadataHandler;
        private final Method method;

        private Object call0(Object target, Command<Object> command) {
            return invoker.apply(target, command);
        }

        private MethodCallCommandHandler copy(Object target) {
            return new MethodCallCommandHandler(target, invoker, metadata, metadataHandler, method);
        }

        private Mono<FunctionMetadata> getMetadata0(Object target, Command<?> cmd) {
            return metadataHandler.apply(target, cmd, metadata);
        }

        @SuppressWarnings("all")
        private Object handle0(Object target,
                               @Nonnull Command<Object> command,
                               @Nonnull CommandSupport support) {
            if (target == null) {
                throw new UnsupportedOperationException("unsupported call not implement method " + method);
            }
            Object result = call0(target, command);
            // 转换flux
            if (command.isWrapperFor(StreamCommand.class)
                || metadata.getExpand(CommandConstant.responseFlux).orElse(false)) {
                return CommandUtils.convertResponseToFlux(result, command);
            }
            return result;
        }

        @Override
        @SuppressWarnings("all")
        public Object handle(@Nonnull Command<Object> command,
                             @Nonnull CommandSupport support) {
            return handle0(target, command, support);
        }

        @Override
        public Mono<FunctionMetadata> getMetadata(Command<?> cmd) {
            return getMetadata0(target, cmd);
        }

        @Nonnull
        @Override
        @SuppressWarnings("all")
        public Command<Object> createCommand() {
            if (invoker.isWrapperFor(CommandInvoker.class)) {
                return invoker
                    .unwrap(CommandInvoker.class)
                    .createCommand();
            }
            if (metadata.getExpand(CommandConstant.responseFlux).orElse(false)) {
                return (Command) new MethodCallFluxCommand(metadata.getId());
            }
            if (CommandConstant.isStream(metadata)) {
                return (Command) new MethodCallStreamCommand(metadata.getId());
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
    public static class MethodCallStreamCommand extends AbstractStreamCommand<Object, Object, MethodCallStreamCommand> {
        private String commandId;

        @Override
        public Object createResponseData(Object value) {
            return value;
        }

        @Override
        public Object convertStreamValue(Object value) {
            return value;
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
