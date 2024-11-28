package org.jetlinks.supports.command;

import com.google.common.collect.Sets;
import org.jetlinks.core.annotation.command.CommandHandler;
import org.jetlinks.core.command.AbstractConvertCommand;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

public class JavaBeanCommandSupportTest {


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
               .expectNextCount(1)
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

    public static class MyBean {

        public String callMultiArg(int val, String val2) {
            System.out.println("callMultiArg(" + val + "," + val2 + ")");
            return val + val2;
        }

        public int callSingleArg(int val) {
            System.out.println("callSingleArg(" + val + ")");
            return val;
        }

        @CommandHandler(TestCommand.class)
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

    public static class TestCommand<T> extends AbstractConvertCommand<Flux<T>, TestCommand<T>> {

    }
}