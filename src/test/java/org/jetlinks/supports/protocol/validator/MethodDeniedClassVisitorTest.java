package org.jetlinks.supports.protocol.validator;

import lombok.SneakyThrows;
import org.hswebframework.web.exception.I18nSupportException;
import org.hswebframework.web.i18n.MessageSourceInitializer;
import org.junit.Test;
import org.springframework.context.support.ResourceBundleMessageSource;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.Duration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MethodDeniedClassVisitorTest {


    static {
        ResourceBundleMessageSource source = new ResourceBundleMessageSource();
        source.setDefaultEncoding("UTF-8");
        source.setBasename("i18n/jetlinks-core/messages");
        MessageSourceInitializer.init(
                source
        );
    }

    @Test
    @SneakyThrows
    public void testBlock() {

        doTest(TestBlock.class);

    }


    @Test
    public void testReflect() {
        MethodDeniedClassVisitor
                .global()
                .addDenied(Method.class, "invoke");
        doTest(TestReflect.class);
    }


    void doTest(Class<?> clazz) {

        try {
            MethodDeniedClassVisitor
                    .global()
                    .validate(
                            clazz.getName(),
                            clazz.getResourceAsStream("/" + clazz
                                    .getName()
                                    .replace(".", "/") + ".class")
                    );
            fail();
        } catch (I18nSupportException e) {
            // out.println(e.getLocalizedMessage());
            e.printStackTrace();
            assertTrue(true);
        }
    }

    static class TestReflect {

        static {
            try {
                Method method = Runtime.class.getMethod("exit", int.class);
                method.invoke(Runtime.class, 1);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }


    static class TestBlock {
        public void test() {
            Mono.delay(Duration.ofSeconds(1))
                .doOnNext((e) -> {
                    Mono.just(1).block(Duration.ofSeconds(1));
                })
                .subscribe();
        }
    }

}