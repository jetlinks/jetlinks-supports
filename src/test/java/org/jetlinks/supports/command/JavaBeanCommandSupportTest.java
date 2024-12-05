package org.jetlinks.supports.command;

import com.google.common.collect.Sets;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetlinks.core.annotation.command.CommandHandler;
import org.jetlinks.core.command.AbstractConvertCommand;
import org.junit.Test;
import org.springframework.web.bind.annotation.RequestBody;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

public class JavaBeanCommandSupportTest {


    public static final String TEST_COMMAND_NAME = "测试命令";
    public static final String TEST_COMMAND_DESCRIPTION = "测试命令描述";

    @Test
    public void testVoid() {

        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callVoid"));

        support.getCommandMetadata("callVoid")
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callVoid", Collections.emptyMap())
               .as(StepVerifier::create)
               .expectComplete()
               .verify();
    }

    @Test
    public void testCallSingleArg() {

        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callSingleArg"));

        support.getCommandMetadata("callSingleArg")
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callSingleArg", Collections.singletonMap("val", "123"))
               .as(StepVerifier::create)
               .expectNext(123)
               .verifyComplete();
    }

    @Test
    public void testCallMultiArg() {

        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callSingleArg"));

        support.getCommandMetadata("callSingleArg")
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callSingleArg", Collections.singletonMap("val", "123"))
               .as(StepVerifier::create)
               .expectNext(123)
               .verifyComplete();

        support.executeToMono("callSingleArg", Collections.singletonMap("val", null))
               .as(StepVerifier::create)
               .expectNext(0)
               .verifyComplete();
    }

    @Test
    public void testCallCommand() {
        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callCommand"));

        support.getCommandMetadata("Test")
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextMatches(f -> f.getName().equals(TEST_COMMAND_NAME)
                   && f.getDescription().equals(TEST_COMMAND_DESCRIPTION))
               .verifyComplete();

        support.executeToMono("Test", Collections.singletonMap("val", "123"))
               .as(StepVerifier::create)
               .expectNext(1)
               .verifyComplete();
    }

    @Test
    public void testIgnore() {
        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("ignore"));

        support.getCommandMetadata("ignore")
               .as(StepVerifier::create)
               .expectNextCount(0)
               .verifyComplete();

    }


    @Test
    public void testBody() {
        MyBean bean = new MyBean();

        JavaBeanCommandSupport support = new JavaBeanCommandSupport(
            bean,
            Sets.newHashSet("callBody"));

        support.getCommandMetadata("callBody")
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callBody", Collections.singletonMap("data", "123"))
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();
    }

    public static class MyBean implements MyBeanApi<TestBody> {

        public String callMultiArg(int val, String val2) {
            System.out.println("callMultiArg(" + val + "," + val2 + ")");
            return val + val2;
        }

        public int callSingleArg(int val) {
            System.out.println("callSingleArg(" + val + ")");
            return val;
        }


        @CommandHandler
        public int callCommand(TestCommand<String> cmd) {
            System.out.println("callCommand(" + cmd + ")");
            System.out.println(cmd.readable());
            return 1;
        }

        @CommandHandler(ignore = true)
        public int ignore(int val) {
            return val;
        }

        public void callVoid() {
            System.out.println("callVoid");
        }
    }

    public interface MyBeanApi<T> {

        default T callBody(@RequestBody T data) {
            System.out.println("callBody(" + data + ")");
            return data;
        }

    }

    @Getter
    @Setter
    @ToString
    public static class TestBody {
        @Schema(title = "Data")
        private int data;
    }

    @Schema(title = TEST_COMMAND_NAME, description = TEST_COMMAND_DESCRIPTION)
    public static class TestCommand<T> extends AbstractConvertCommand<Flux<T>, TestCommand<T>> {

    }
}