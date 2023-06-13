package org.jetlinks.supports.protocol.validator;

import lombok.Getter;
import org.hswebframework.web.exception.I18nSupportException;

@Getter
public class MethodInvokeDeniedException extends I18nSupportException {
    private final String className;

    private final String methodName;

    private final String calledClassName;

    private final String calledMethodName;
    private final int lineNumber;

    public MethodInvokeDeniedException(String className, String methodName, String calledClassName, String calledMethodName, int lineNumber) {
        super("error.method.call.denied",
              parseClassSimpleName(calledClassName),
              calledMethodName,
              parseClassSimpleName(className),
              lineNumber);

        this.className = className;
        this.methodName = methodName;
        this.calledClassName = calledClassName;
        this.calledMethodName = calledMethodName;
        this.lineNumber = lineNumber;
    }


    private static String parseClassSimpleName(String name) {
        if (name.contains(".")) {
            return name.substring(name.lastIndexOf(".") + 1);
        }
        return name;
    }

}
