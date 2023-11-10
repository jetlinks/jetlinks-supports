package org.jetlinks.supports.command;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import org.hswebframework.web.aop.MethodInterceptorHolder;
import org.hswebframework.web.bean.FastBeanCopier;
import org.jetlinks.core.command.AbstractCommand;
import org.jetlinks.core.command.AbstractCommandSupport;
import org.jetlinks.core.command.Command;
import org.jetlinks.core.command.CommandHandler;
import org.jetlinks.core.metadata.DataType;
import org.jetlinks.core.metadata.PropertyMetadata;
import org.jetlinks.core.metadata.types.ObjectType;
import org.jetlinks.supports.official.DeviceMetadataParser;
import org.jetlinks.supports.official.JetLinksDeviceFunctionMetadata;
import org.jetlinks.supports.official.JetLinksPropertyMetadata;
import org.springframework.beans.MethodInvocationException;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
        = method -> !Modifier.isStatic(method.getModifiers())
        && Modifier.isPublic(method.getModifiers())
        && method.getDeclaringClass() != Object.class;

    private final Object target;

    public JavaBeanCommandSupport(Object target, Collection<String> filter) {
        this(target, m -> filter.contains(m.getName()));
    }

    public JavaBeanCommandSupport(Object target, Predicate<Method> filter) {
        this.target = target;
        init(defaultFilter.and(filter));
    }


    private void init(Predicate<Method> filter) {
        ReflectionUtils
            .doWithMethods(
                ClassUtils.getUserClass(target),
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
    private Object doInvoke(Method method, Object... args) {
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

    private void register(Method method) {
        Schema schema = AnnotationUtils.findAnnotation(method, Schema.class);

        String name = schema != null && StringUtils.hasText(schema.name()) ? schema.name() : method.getName();

        ResolvableType[] argTypes = new ResolvableType[method.getParameterCount()];
        String[] argNames = new String[method.getParameterCount()];
        List<PropertyMetadata> inputs = new ArrayList<>();
        DataType output = null;
        Function<Map<String, Object>, Object> invoker = null;

        for (int i = 0; i < method.getParameterCount(); i++) {
            argTypes[i] = ResolvableType.forMethodParameter(method, i);
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

        ResolvableType returnType = ResolvableType.forMethodReturnType(method);
        if (!returnIsVoid(returnType)) {
            output = DeviceMetadataParser.withType(returnType);
        }
        if (argTypes.length == 0) {
            invoker = ignore -> doInvoke(method);
        } else {
            if (argTypes.length == 1) {
                RequestBody requestBody = AnnotationUtils.findAnnotation(method.getParameters()[0], RequestBody.class);
                if (requestBody != null) {
                    DataType metadataType = DeviceMetadataParser.withType(argTypes[0]);
                    if (metadataType instanceof ObjectType) {
                        inputs = ((ObjectType) metadataType).getProperties();
                    }
                    invoker = parameter -> {
                        ResolvableType type = argTypes[0];
                        return doInvoke(
                            method,
                            FastBeanCopier.DEFAULT_CONVERT.convert(parameter, type.toClass(), type.resolveGenerics())
                        );
                    };
                }
            }
            if (invoker == null) {
                for (int i = 0; i < argTypes.length; i++) {
                    ResolvableType type = argTypes[i];
                    Parameter parameter = parameters[i];
                    Schema schemaAnn = parameter.getAnnotation(Schema.class);
                    DataType dataType = DeviceMetadataParser.withType(type);
                    JetLinksPropertyMetadata metadata = new JetLinksPropertyMetadata();
                    metadata.setId(argNames[i]);
                    metadata.setName(schemaAnn != null && StringUtils.hasText(schemaAnn.description())
                                         ? schemaAnn.description()
                                         : argNames[i]);
                    metadata.setDataType(dataType);
                    inputs.add(metadata);
                }
                invoker = parameter -> {
                    Object[] args = new Object[argTypes.length];
                    for (int i = 0; i < argTypes.length; i++) {
                        ResolvableType type = argTypes[i];
                        Object val = parameter.get(argNames[i]);
                        if (!type.isInstance(val)) {
                            val = FastBeanCopier.DEFAULT_CONVERT.convert(val, type.toClass(), type.resolveGenerics());
                        }
                        args[i] = val;
                    }
                    return doInvoke(method, args);
                };
            }
        }

        JetLinksDeviceFunctionMetadata metadata = new JetLinksDeviceFunctionMetadata();
        metadata.setId(name);
        metadata.setOutput(output);
        metadata.setInputs(inputs);
        metadata.setName(schema != null && StringUtils.hasText(schema.title()) ? schema.title() : name);
        metadata.setDescription(schema == null ? name : schema.description());

        Function<Map<String, Object>, Object> finalInvoker = invoker;

        registerHandler(
            name,
            CommandHandler.of(
                metadata,
                (cmd, self) -> finalInvoker.apply(cmd.readable()),
                () -> new MethodCallCommand(name))
        );

    }


    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MethodCallCommand extends AbstractCommand<Object, MethodCallCommand> {
        private String commandId;
    }
}
