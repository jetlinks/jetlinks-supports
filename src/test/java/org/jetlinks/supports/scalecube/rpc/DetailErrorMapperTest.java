package org.jetlinks.supports.scalecube.rpc;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.*;
import org.junit.Before;
import org.junit.Test;

import static io.scalecube.services.api.ServiceMessage.HEADER_ERROR_TYPE;
import static org.junit.Assert.*;

public class DetailErrorMapperTest {

    private DetailErrorMapper mapper;

    @Before
    public void setUp() {
        mapper = new DetailErrorMapper();
    }

    @Test
    public void testToErrorWithBadRequestException() {
        int errorCode = 400;
        String errorMessage = "Bad Request";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(BadRequestException.ERROR_TYPE))
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof BadRequestException);
        assertEquals(errorCode, ((ServiceException) error).errorCode());
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    public void testToErrorWithUnauthorizedException() {
        int errorCode = 401;
        String errorMessage = "Unauthorized";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(UnauthorizedException.ERROR_TYPE))
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof UnauthorizedException);
        assertEquals(errorCode, ((ServiceException) error).errorCode());
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    public void testToErrorWithForbiddenException() {
        int errorCode = 403;
        String errorMessage = "Forbidden";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(ForbiddenException.ERROR_TYPE))
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof ForbiddenException);
        assertEquals(errorCode, ((ServiceException) error).errorCode());
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    public void testToErrorWithServiceUnavailableException() {
        int errorCode = 503;
        String errorMessage = "Service Unavailable";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(ServiceUnavailableException.ERROR_TYPE))
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof ServiceUnavailableException);
        assertEquals(errorCode, ((ServiceException) error).errorCode());
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    public void testToErrorWithInternalServiceException() {
        int errorCode = 500;
        String errorMessage = "Internal Server Error";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(InternalServiceException.ERROR_TYPE))
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof InternalServiceException);
        assertEquals(errorCode, ((ServiceException) error).errorCode());
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    public void testToErrorWithUnknownErrorType() {
        int errorCode = 500;
        String errorMessage = "Unknown Error";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, "999")
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof InternalServiceException);
        assertEquals(errorCode, ((ServiceException) error).errorCode());
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    public void testToErrorWithErrorInfo() {
        int errorCode = 500;
        String errorMessage = "Test Error";
        RuntimeException originalError = new RuntimeException("Original error message");
        String encodedInfo = DetailErrorMapper.encodeInfo(originalError);

        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, "999")
            .header(DetailErrorMapper.ERROR_INFO, encodedInfo)
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertEquals("Original error message", error.getMessage());
    }

    @Test
    public void testToErrorWithErrorDetail() {
        int errorCode = 400;
        String errorMessage = "Bad Request";
        RuntimeException testError = new RuntimeException("Test error");
        String encodedDetail = DetailErrorMapper.createDetail(new StackTraceElement[0], testError);

        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(BadRequestException.ERROR_TYPE))
            .header(DetailErrorMapper.ERROR_DETAIL, encodedDetail)
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof BadRequestException);
        StackTraceElement[] stackTrace = error.getStackTrace();
        assertNotNull(stackTrace);
        assertTrue(stackTrace.length > 0);
    }

    @Test
    public void testToErrorWithTopTrace() {
        int errorCode = 400;
        String errorMessage = "Bad Request";
        StackTraceElement[] topTrace = {
            new StackTraceElement("TopClass", "topMethod", "TopFile.java", 10)
        };
        mapper.setTopTrace(topTrace);

        RuntimeException testError = new RuntimeException("Test error");
        String encodedDetail = DetailErrorMapper.createDetail(new StackTraceElement[0], testError);

        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(BadRequestException.ERROR_TYPE))
            .header(DetailErrorMapper.ERROR_DETAIL, encodedDetail)
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        StackTraceElement[] stackTrace = error.getStackTrace();
        assertNotNull(stackTrace);
        assertTrue(stackTrace.length > 0);
        assertEquals("TopClass", stackTrace[0].getClassName());
        assertEquals("topMethod", stackTrace[0].getMethodName());
    }

    @Test
    public void testToMessageWithBadRequestException() {
        String qualifier = "test.qualifier";
        BadRequestException exception = new BadRequestException(400, "Bad Request");

        ServiceMessage message = mapper.toMessage(qualifier, exception);

        assertNotNull(message);
        assertEquals(qualifier, message.qualifier());
        assertEquals(String.valueOf(BadRequestException.ERROR_TYPE), message.header(HEADER_ERROR_TYPE));
        ErrorData errorData = message.data();
        assertNotNull(errorData);
        assertEquals(400, errorData.getErrorCode());
        assertEquals("Bad Request", errorData.getErrorMessage());
        assertNotNull(message.header(DetailErrorMapper.ERROR_INFO));
        assertNotNull(message.header(DetailErrorMapper.ERROR_DETAIL));
    }

    @Test
    public void testToMessageWithUnauthorizedException() {
        String qualifier = "test.qualifier";
        UnauthorizedException exception = new UnauthorizedException(401, "Unauthorized");

        ServiceMessage message = mapper.toMessage(qualifier, exception);

        assertNotNull(message);
        assertEquals(String.valueOf(UnauthorizedException.ERROR_TYPE), message.header(HEADER_ERROR_TYPE));
        ErrorData errorData = message.data();
        assertEquals(401, errorData.getErrorCode());
        assertEquals("Unauthorized", errorData.getErrorMessage());
    }

    @Test
    public void testToMessageWithForbiddenException() {
        String qualifier = "test.qualifier";
        ForbiddenException exception = new ForbiddenException(403, "Forbidden");

        ServiceMessage message = mapper.toMessage(qualifier, exception);

        assertNotNull(message);
        assertEquals(String.valueOf(ForbiddenException.ERROR_TYPE), message.header(HEADER_ERROR_TYPE));
        ErrorData errorData = message.data();
        assertEquals(403, errorData.getErrorCode());
        assertEquals("Forbidden", errorData.getErrorMessage());
    }

    @Test
    public void testToMessageWithServiceUnavailableException() {
        String qualifier = "test.qualifier";
        ServiceUnavailableException exception = new ServiceUnavailableException(503, "Service Unavailable");

        ServiceMessage message = mapper.toMessage(qualifier, exception);

        assertNotNull(message);
        assertEquals(String.valueOf(ServiceUnavailableException.ERROR_TYPE), message.header(HEADER_ERROR_TYPE));
        ErrorData errorData = message.data();
        assertEquals(503, errorData.getErrorCode());
        assertEquals("Service Unavailable", errorData.getErrorMessage());
    }

    @Test
    public void testToMessageWithInternalServiceException() {
        String qualifier = "test.qualifier";
        InternalServiceException exception = new InternalServiceException(500, "Internal Server Error");

        ServiceMessage message = mapper.toMessage(qualifier, exception);

        assertNotNull(message);
        assertEquals(String.valueOf(500), message.header(HEADER_ERROR_TYPE));
        ErrorData errorData = message.data();
        assertEquals(500, errorData.getErrorCode());
        assertEquals("Internal Server Error", errorData.getErrorMessage());
    }


    @Test
    public void testToMessageWithExceptionWithoutMessage() {
        String qualifier = "test.qualifier";
        NullPointerException exception = new NullPointerException();

        ServiceMessage message = mapper.toMessage(qualifier, exception);

        assertNotNull(message);
        ErrorData errorData = message.data();
        assertNotNull(errorData.getErrorMessage());
        assertFalse(errorData.getErrorMessage().isEmpty());
    }

    @Test
    public void testEncodeAndDecodeInfo() {
        RuntimeException originalError = new RuntimeException("Test error message");
        originalError.initCause(new IllegalArgumentException("Cause error"));

        String encoded = DetailErrorMapper.encodeInfo(originalError);
        assertNotNull(encoded);
        assertFalse(encoded.isEmpty());

        Throwable decoded = DetailErrorMapper.decodeInfo(encoded);
        assertNotNull(decoded);
        decoded.printStackTrace();
        assertEquals("Test error message", decoded.getMessage());

    }

    @Test
    public void testDecodeInfoWithNull() {
        Throwable decoded = DetailErrorMapper.decodeInfo(null);
        assertNull(decoded);
    }

    @Test
    public void testDecodeInfoWithEmptyString() {
        Throwable decoded = DetailErrorMapper.decodeInfo("");
        assertNull(decoded);
    }

    @Test
    public void testDecodeInfoWithInvalidBase64() {
        Throwable decoded = DetailErrorMapper.decodeInfo("invalid-base64-string!!!");
        assertNull(decoded);
    }

    @Test
    public void testEncodeInfoWithNull() {
        String encoded = DetailErrorMapper.encodeInfo(null);
        assertNotNull(encoded);
        assertEquals("", encoded);
    }

    @Test
    public void testCreateAndDecodeDetail() {
        RuntimeException testError = new RuntimeException("Test error");
        testError.fillInStackTrace();

        String encoded = DetailErrorMapper.createDetail(new StackTraceElement[0], testError);
        assertNotNull(encoded);
        assertFalse(encoded.isEmpty());

        StackTraceElement[] decoded = DetailErrorMapper.decodeDetail(encoded);
        assertNotNull(decoded);
        assertTrue(decoded.length > 0);
    }

    @Test
    public void testCreateDetailWithEmptyStackTrace() {
        RuntimeException testError = new RuntimeException("Test error");
        testError.setStackTrace(new StackTraceElement[0]);

        String encoded = DetailErrorMapper.createDetail(new StackTraceElement[0], testError);
        assertNotNull(encoded);
        assertEquals("", encoded);
    }

    @Test
    public void testDecodeDetailWithNull() {
        StackTraceElement[] decoded = DetailErrorMapper.decodeDetail(null);
        assertNull(decoded);
    }

    @Test
    public void testDecodeDetailWithEmptyString() {
        StackTraceElement[] decoded = DetailErrorMapper.decodeDetail("");
        assertNull(decoded);
    }

    @Test
    public void testDecodeDetailWithInvalidBase64() {
        StackTraceElement[] decoded = DetailErrorMapper.decodeDetail("invalid-base64-string!!!");
        assertNull(decoded);
    }

    @Test
    public void testCreateDetailWithTopTrace() {
        RuntimeException testError = new RuntimeException("Test error");
        testError.fillInStackTrace();

        StackTraceElement[] topTrace = {
            new StackTraceElement("TopClass", "topMethod", "TopFile.java", 10),
            new StackTraceElement("TopClass2", "topMethod2", "TopFile2.java", 20)
        };

        String encoded = DetailErrorMapper.createDetail(topTrace, testError);
        assertNotNull(encoded);
        assertFalse(encoded.isEmpty());

        StackTraceElement[] decoded = DetailErrorMapper.decodeDetail(encoded, topTrace);
        assertNotNull(decoded);
        assertTrue(decoded.length >= topTrace.length);
        assertEquals("TopClass", decoded[0].getClassName());
        assertEquals("topMethod", decoded[0].getMethodName());
    }

    @Test
    public void testDecodeDetailWithTopTrace() {
        RuntimeException testError = new RuntimeException("Test error");
        testError.fillInStackTrace();

        StackTraceElement[] topTrace = {
            new StackTraceElement("TopClass", "topMethod", "TopFile.java", 10)
        };

        String encoded = DetailErrorMapper.createDetail(new StackTraceElement[0], testError);
        StackTraceElement[] decoded = DetailErrorMapper.decodeDetail(encoded, topTrace);

        assertNotNull(decoded);
        assertTrue(decoded.length > topTrace.length);
        assertEquals("TopClass", decoded[0].getClassName());
    }

    @Test
    public void testRoundTripConversion() {
        String qualifier = "test.qualifier";
        BadRequestException originalException = new BadRequestException(400, "Original error");
        originalException.fillInStackTrace();

        // Convert exception to message
        ServiceMessage message = mapper.toMessage(qualifier, originalException);

        // Convert message back to exception
        Throwable convertedException = mapper.toError(message);

        assertNotNull(convertedException);
        assertTrue(convertedException instanceof BadRequestException);
        assertEquals(400, ((ServiceException) convertedException).errorCode());
        assertEquals("Original error", convertedException.getMessage());
    }

    @Test
    public void testRoundTripWithNonServiceException() {
        String qualifier = "test.qualifier";
        IllegalArgumentException originalException = new IllegalArgumentException("Illegal argument");
        originalException.fillInStackTrace();

        // Convert exception to message
        ServiceMessage message = mapper.toMessage(qualifier, originalException);

        // Convert message back to exception
        Throwable convertedException = mapper.toError(message);

        assertNotNull(convertedException);
        // Should decode from ERROR_INFO header
        assertEquals("Illegal argument", convertedException.getMessage());
    }

    @Test
    public void testInstance() {
        assertNotNull(DetailErrorMapper.INSTANCE);
        assertSame(DetailErrorMapper.INSTANCE, DetailErrorMapper.INSTANCE);
    }

    @Test
    public void testToErrorWithNullErrorDetail() {
        int errorCode = 400;
        String errorMessage = "Bad Request";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, String.valueOf(BadRequestException.ERROR_TYPE))
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof BadRequestException);
    }

    @Test
    public void testToErrorWithNullErrorInfo() {
        int errorCode = 500;
        String errorMessage = "Unknown Error";
        ServiceMessage message = ServiceMessage
            .builder()
            .header(HEADER_ERROR_TYPE, "999")
            .data(new ErrorData(errorCode, errorMessage))
            .build();

        Throwable error = mapper.toError(message);

        assertNotNull(error);
        assertTrue(error instanceof InternalServiceException);
        assertEquals(errorCode, ((ServiceException) error).errorCode());
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    public void testCreateDetailWithNullFileName() {
        RuntimeException testError = new RuntimeException("Test error");
        testError.fillInStackTrace();

        // Create a stack trace element with null file name
        StackTraceElement[] customTrace = {
            new StackTraceElement("TestClass", "testMethod", null, 10)
        };

        String encoded = DetailErrorMapper.createDetail(customTrace, testError);
        assertNotNull(encoded);

        StackTraceElement[] decoded = DetailErrorMapper.decodeDetail(encoded);
        assertNotNull(decoded);
        assertTrue(decoded.length > 0);
    }
}

