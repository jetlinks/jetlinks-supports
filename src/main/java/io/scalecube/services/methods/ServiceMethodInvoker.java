package io.scalecube.services.methods;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import lombok.Setter;
import org.jetlinks.core.rpc.ContextCodec;
import org.jetlinks.core.trace.TraceHolder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static io.scalecube.services.auth.Authenticator.AUTH_CONTEXT_KEY;
import static io.scalecube.services.auth.Authenticator.NULL_AUTH_CONTEXT;
import static org.jetlinks.supports.scalecube.rpc.ScalecubeRpcManager.HEADER_CONTEXT;

public final class ServiceMethodInvoker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMethodInvoker.class);

    private final Method method;
    private final Object service;
    private final MethodInfo methodInfo;
    private final ServiceProviderErrorMapper errorMapper;
    private final ServiceMessageDataDecoder dataDecoder;
    private final Authenticator<Object> authenticator;
    private final PrincipalMapper<Object, Object> principalMapper;
    @Setter
    private ContextCodec contextCodec = ContextCodec.DEFAULT;

    /**
     * Constructs a service method invoker out of real service object instance and method info.
     *
     * @param method          service method (required)
     * @param service         service instance (required)
     * @param methodInfo      method information (required)
     * @param errorMapper     error mapper (required)
     * @param dataDecoder     data decoder (required)
     * @param authenticator   authenticator (optional)
     * @param principalMapper principal mapper (optional)
     */
    public ServiceMethodInvoker(
        Method method,
        Object service,
        MethodInfo methodInfo,
        ServiceProviderErrorMapper errorMapper,
        ServiceMessageDataDecoder dataDecoder,
        Authenticator<Object> authenticator,
        PrincipalMapper<Object, Object> principalMapper) {
        this.method = Objects.requireNonNull(method, "method");
        this.service = Objects.requireNonNull(service, "service");
        this.methodInfo = Objects.requireNonNull(methodInfo, "methodInfo");
        this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
        this.dataDecoder = Objects.requireNonNull(dataDecoder, "dataDecoder");
        this.authenticator = authenticator;
        this.principalMapper = principalMapper;
    }

    /**
     * Invokes service method with single response.
     *
     * @param message request service message
     * @return mono of service message
     */
    public Mono<ServiceMessage> invokeOne(ServiceMessage message) {
        return Mono.deferContextual(context -> authenticate(message, context))
                   .flatMap(authData -> deferWithContextOne(message, authData))
                   .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
                   .onErrorResume(
                       throwable -> Mono.just(errorMapper.toMessage(message.qualifier(), throwable)));
    }

    /**
     * Invokes service method with message stream response.
     *
     * @param message request service message
     * @return flux of service messages
     */
    public Flux<ServiceMessage> invokeMany(ServiceMessage message) {
        return Mono.deferContextual(context -> authenticate(message, context))
                   .flatMapMany(authData -> deferWithContextMany(message, authData))
                   .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
                   .onErrorResume(
                       throwable -> Flux.just(errorMapper.toMessage(message.qualifier(), throwable)));
    }

    /**
     * Invokes service method with bidirectional communication.
     *
     * @param publisher request service message
     * @return flux of service messages
     */
    public Flux<ServiceMessage> invokeBidirectional(Publisher<ServiceMessage> publisher) {

        return Flux
            .deferContextual(ctx -> Flux
                .from(publisher)
                .switchOnFirst((s, flux) -> {
                    ServiceMessage first;
                    if (s.hasValue() && (first = s.get()) != null) {
                        return this
                            // auth
                            .authenticate(first, ctx)
                            //invoke
                            .flatMapMany(authData -> flux
                                .map(this::toRequest)
                                .transform(this::invoke)
                                .contextWrite(context -> enhanceContext(authData, context)))
                            //trace
                            .contextWrite(TraceHolder.readToContext(s.getContextView(), first.headers()))
                            //response
                            .map(response -> toResponse(response, first.qualifier(), first.dataFormat()))
                            //error
                            .onErrorResume(throwable -> Flux.just(errorMapper.toMessage(first.qualifier(), throwable)));
                    }
                    return flux.then(Mono.empty());
                }));
//        return Flux.from(publisher)
//                   .switchOnFirst(
//                       (first, messages) ->
//                           Mono.deferContextual(context -> authenticate(first.get(), context))
//                               .flatMapMany(authData -> deferWithContextBidirectional(messages, authData))
//                               .map(
//                                   response ->
//                                       toResponse(response,
//                                                  first.get().qualifier(),
//                                                  first.get().dataFormat()))
//                               .onErrorResume(
//                                   throwable ->
//                                       Flux.just(errorMapper.toMessage(first.get().qualifier(), throwable))));
    }

    private Context decodeContext(Context ctx, ServiceMessage message) {
        ctx = TraceHolder
            .readToContext(
                ctx,
                message.headers());
        String context = message.header(HEADER_CONTEXT);
        if (context != null) {
            ctx = contextCodec.deserialize(ctx,() -> context);
        }
        return ctx;
    }

    private Mono<?> deferWithContextOne(ServiceMessage message, Object authData) {
        return Mono.from(invoke(toRequest(message)))
                   .contextWrite(context -> decodeContext(enhanceContext(authData, context), message));
    }

    private Flux<?> deferWithContextMany(ServiceMessage message, Object authData) {
        return Flux.from(invoke(toRequest(message)))
                   .contextWrite(context -> decodeContext(enhanceContext(authData, context), message));
    }

    private Flux<?> deferWithContextBidirectional(Flux<ServiceMessage> messages, Object authData) {
        return messages
            .switchOnFirst((s, flux) -> {
                ServiceMessage msg;
                if (s.hasValue() && (msg = s.get()) != null) {
                    return flux
                        .map(this::toRequest)
                        .transform(this::invoke)
                        .contextWrite(context -> decodeContext(enhanceContext(authData, context), msg));
                }
                return flux;
            })
            .contextWrite(context -> enhanceContext(authData, context));
    }

    private Publisher<?> invoke(Object request) {
        Publisher<?> result = null;
        Throwable throwable = null;
        try {
            if (methodInfo.parameterCount() == 0) {
                result = (Publisher<?>) method.invoke(service);
            } else {
                Object[] arguments = prepareArguments(request);
                result = (Publisher<?>) method.invoke(service, arguments);
            }
            if (result == null) {
                result = Mono.empty();
            }
        } catch (InvocationTargetException ex) {
            throwable = Optional.ofNullable(ex.getCause()).orElse(ex);
        } catch (Throwable ex) {
            throwable = ex;
        }
        return throwable != null ? Mono.error(throwable) : result;
    }

    private Object[] prepareArguments(Object request) {
        Object[] arguments = new Object[methodInfo.parameterCount()];
        if (methodInfo.requestType() != Void.TYPE) {
            arguments[0] = request;
        }
        return arguments;
    }

    private Mono<Object> authenticate(ServiceMessage message, ContextView context) {
        if (!methodInfo.isSecured()) {
            return Mono.just(NULL_AUTH_CONTEXT);
        }

        if (authenticator == null) {
            if (context.hasKey(AUTH_CONTEXT_KEY)) {
                return Mono.just(context.get(AUTH_CONTEXT_KEY));
            } else {
                LOGGER.error("Authentication failed (auth context not found and authenticator not set)");
                throw new UnauthorizedException("Authentication failed");
            }
        }

        return authenticator
            .apply(message.headers())
            .switchIfEmpty(Mono.just(NULL_AUTH_CONTEXT))
            .onErrorMap(this::toUnauthorizedException);
    }

    private UnauthorizedException toUnauthorizedException(Throwable th) {
        if (th instanceof ServiceException) {
            ServiceException e = (ServiceException) th;
            return new UnauthorizedException(e.errorCode(), e.getMessage());
        } else {
            return new UnauthorizedException(th);
        }
    }

    private Context enhanceContext(Object authData, Context context) {
        if (authData == NULL_AUTH_CONTEXT || principalMapper == null) {
            return context.put(AUTH_CONTEXT_KEY, authData);
        }

        Object mappedData = principalMapper.apply(authData);

        return context.put(AUTH_CONTEXT_KEY, mappedData != null ? mappedData : NULL_AUTH_CONTEXT);
    }

    private Object toRequest(ServiceMessage message) {
        ServiceMessage request = dataDecoder.apply(message, methodInfo.requestType());

        if (!methodInfo.isRequestTypeVoid()
            && !methodInfo.isRequestTypeServiceMessage()
            && !request.hasData(methodInfo.requestType())) {

            Optional<?> dataOptional = Optional.ofNullable(request.data());
            Class<?> clazz = dataOptional.map(Object::getClass).orElse(null);
            throw new BadRequestException(
                String.format(
                    "Expected service request data of type: %s, but received: %s",
                    methodInfo.requestType(), clazz));
        }

        return methodInfo.isRequestTypeServiceMessage() ? request : request.data();
    }

    private ServiceMessage toResponse(Object response, String qualifier, String dataFormat) {
        if (response instanceof ServiceMessage) {
            ServiceMessage message = (ServiceMessage) response;
            if (dataFormat != null && !dataFormat.equals(message.dataFormat())) {
                return ServiceMessage.from(message).qualifier(qualifier).dataFormat(dataFormat).build();
            }
            return ServiceMessage.from(message).qualifier(qualifier).build();
        }
        return ServiceMessage.builder()
                             .qualifier(qualifier)
                             .data(response)
                             .dataFormatIfAbsent(dataFormat)
                             .build();
    }

    public Object service() {
        return service;
    }

    public MethodInfo methodInfo() {
        return methodInfo;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ServiceMethodInvoker.class.getSimpleName() + "[", "]")
            .add("method=" + method)
            .add("service=" + service)
            .add("methodInfo=" + methodInfo)
            .add("errorMapper=" + errorMapper)
            .add("dataDecoder=" + dataDecoder)
            .add("authenticator=" + authenticator)
            .add("principalMapper=" + principalMapper)
            .toString();
    }
}
