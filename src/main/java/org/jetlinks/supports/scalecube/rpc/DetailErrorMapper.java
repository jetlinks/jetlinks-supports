package org.jetlinks.supports.scalecube.rpc;

import io.netty.util.concurrent.FastThreadLocal;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.*;
import org.jetlinks.core.utils.ExceptionUtils;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;

import static io.scalecube.services.api.ServiceMessage.HEADER_ERROR_TYPE;

public class DetailErrorMapper implements ServiceClientErrorMapper, ServiceProviderErrorMapper {
    static final FastThreadLocal<ByteArrayOutputStream> SHARED_OUT = new FastThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream(1024);
        }
    };


    private static final int DEFAULT_ERROR_CODE = 500;

    public static final DetailErrorMapper INSTANCE = new DetailErrorMapper();
    public static final String ERROR_DETAIL = "errorDetail";


    StackTraceElement[] topTrace = new StackTraceElement[0];

    void setTopTrace(StackTraceElement... trace) {
        topTrace = trace;
    }

    @Override
    public Throwable toError(ServiceMessage message) {
        ErrorData data = message.data();

        int errorType = message.errorType();
        int errorCode = data.getErrorCode();
        String errorMessage = data.getErrorMessage();
        StackTraceElement[] stackTrace = decodeDetail(message.header(ERROR_DETAIL), topTrace);

        Throwable error;
        switch (errorType) {
            case BadRequestException.ERROR_TYPE:
                error = new BadRequestException(errorCode, errorMessage);
                break;
            case UnauthorizedException.ERROR_TYPE:
                error = new UnauthorizedException(errorCode, errorMessage);
                break;
            case ForbiddenException.ERROR_TYPE:
                error = new ForbiddenException(errorCode, errorMessage);
                break;
            case ServiceUnavailableException.ERROR_TYPE:
                error = new ServiceUnavailableException(errorCode, errorMessage);
                break;
            case InternalServiceException.ERROR_TYPE:
                error = new InternalServiceException(errorCode, errorMessage);
                break;
            // Handle other types of Service Exceptions here
            default:
                error = new InternalServiceException(errorCode, errorMessage);
        }
        if (stackTrace != null) {
            error.setStackTrace(stackTrace);
        }
        return error;

    }

    public static StackTraceElement[] decodeDetail(String e, StackTraceElement... top) {
        if (!StringUtils.hasText(e)) {
            return null;
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(e)))) {
            int length = input.readInt();
            StackTraceElement[] stack = Arrays.copyOf(top, top.length + length);
            for (int i = top.length; i < length + top.length; i++) {
                String className = input.readUTF();
                String methodName = input.readUTF();
                String fileName = input.readUTF();
                int lineNumber = input.readInt();
                stack[i] = new StackTraceElement(className, methodName, StringUtils.hasText(fileName) ? fileName : null, lineNumber);
            }
            return stack;
        } catch (Throwable ignore) {
            return null;
        }
    }


    public static String createDetail(StackTraceElement[] top, Throwable e) {
        StackTraceElement[] stack = ExceptionUtils.getMergedStackTrace(e);

        if (stack.length == 0) {
            return "";
        }
        ByteArrayOutputStream out = SHARED_OUT.get();
        try (DataOutputStream dataOut = new DataOutputStream(out)) {
            dataOut.writeInt(stack.length + top.length);

            for (StackTraceElement element : top) {
                dataOut.writeUTF(element.getClassName());
                dataOut.writeUTF(element.getMethodName());
                dataOut.writeUTF(element.getFileName() == null ? "" : element.getFileName());
                dataOut.writeInt(element.getLineNumber());
            }
            for (StackTraceElement element : stack) {
                dataOut.writeUTF(element.getClassName());
                dataOut.writeUTF(element.getMethodName());
                dataOut.writeUTF(element.getFileName() == null ? "" : element.getFileName());
                dataOut.writeInt(element.getLineNumber());
            }
            return Base64.getEncoder().encodeToString(out.toByteArray());
        } catch (Throwable ignore) {
            return "";
        } finally {
            out.reset();
        }
    }

    @Override
    public ServiceMessage toMessage(String qualifier, Throwable throwable) {
        int errorCode = DEFAULT_ERROR_CODE;
        int errorType = DEFAULT_ERROR_CODE;

        if (throwable instanceof ServiceException) {
            errorCode = ((ServiceException) throwable).errorCode();
            if (throwable instanceof BadRequestException) {
                errorType = BadRequestException.ERROR_TYPE;
            } else if (throwable instanceof UnauthorizedException) {
                errorType = UnauthorizedException.ERROR_TYPE;
            } else if (throwable instanceof ForbiddenException) {
                errorType = ForbiddenException.ERROR_TYPE;
            } else if (throwable instanceof ServiceUnavailableException) {
                errorType = ServiceUnavailableException.ERROR_TYPE;
            }
        }

        String errorMessage = Optional
            .ofNullable(throwable.getMessage())
            .orElseGet(throwable::toString);

        return ServiceMessage
            .builder()
            .qualifier(qualifier)
            .header(HEADER_ERROR_TYPE, String.valueOf(errorType))
            .header(ERROR_DETAIL, createDetail(topTrace, throwable))
            .data(new ErrorData(errorCode, errorMessage))
            .build();
    }
}
