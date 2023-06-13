package org.jetlinks.supports.protocol.validator;

import lombok.SneakyThrows;
import org.hswebframework.web.exception.I18nSupportException;
import org.hswebframework.web.i18n.MessageSourceInitializer;
import org.junit.Test;
import org.springframework.context.support.ResourceBundleMessageSource;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MethodDeniedClassVisitorTest {

    @Test
    @SneakyThrows
    public void testBlock() {

        ResourceBundleMessageSource source = new ResourceBundleMessageSource();
        source.setDefaultEncoding("UTF-8");
        source.setBasename("i18n/jetlinks-core/messages");
        MessageSourceInitializer.init(
                source
        );
        MethodDeniedClassVisitor visitor = new MethodDeniedClassVisitor();
        visitor.addDefaultDenied();

        try {
            visitor.validate(
                    TestBlock.class.getName(),
                    TestBlock.class.getResourceAsStream("/" + TestBlock.class.getName().replace(".", "/") + ".class")
            );
            fail();
        } catch (I18nSupportException e) {
            // out.println(e.getLocalizedMessage());
            e.printStackTrace();
            assertTrue(true);
        }

    }

    static Mono<Integer> getData() {
        return Mono.just(1);
    }

    static class TestBlock {
        public void test() {
            Mono.delay(Duration.ofSeconds(1))
                .doOnNext((e) -> {
                    getData().block();
                })
                .subscribe();
        }
    }

}