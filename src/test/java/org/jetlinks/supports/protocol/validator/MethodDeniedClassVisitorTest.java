package org.jetlinks.supports.protocol.validator;

import lombok.SneakyThrows;
import org.hswebframework.web.exception.I18nSupportException;
import org.hswebframework.web.i18n.LocaleUtils;
import org.hswebframework.web.i18n.MessageSourceInitializer;
import org.junit.Test;
import org.springframework.asm.ClassReader;
import org.springframework.context.support.ResourceBundleMessageSource;
import reactor.core.publisher.Mono;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static java.lang.System.out;
import static org.junit.Assert.*;

public class MethodDeniedClassVisitorTest {

    @Test
    @SneakyThrows
    public void testBlock() {

        ResourceBundleMessageSource source=new ResourceBundleMessageSource();
        source.setDefaultEncoding("UTF-8");
        source.setBasename("i18n/jetlinks-core/messages");
        MessageSourceInitializer.init(
                source
        );
        MethodDeniedClassVisitor visitor = new MethodDeniedClassVisitor();
        visitor.addDefaultDenied();

        try {
            visitor.validate(TestBlock.class.getName(),
                             new FileInputStream(
                                     "/Users/zhouhao/IdeaProjects/jetlinks-v2/jetlinks-cloud/dev/jetlinks-official-protocol/target/classes/org/jetlinks/protocol/official/JetLinksCoapDTLSDeviceMessageCodec.class"
                             ));
            fail();
        } catch (I18nSupportException e) {
           // out.println(e.getLocalizedMessage());
            e.printStackTrace();
            assertTrue(true);
        }

    }

    static class TestBlock {
        public void test() {
            Mono.delay(Duration.ofSeconds(1))
                .doOnNext((e) -> {
                    Mono.just(1).block();
                })
                .subscribe();
        }
    }

}