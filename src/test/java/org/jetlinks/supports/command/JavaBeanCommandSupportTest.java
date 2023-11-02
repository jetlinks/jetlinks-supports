package org.jetlinks.supports.command;

import com.google.common.collect.Sets;
import org.junit.Test;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.junit.Assert.*;

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
            Sets.newHashSet("callMultiArg"));

        support.getCommandMetadata("callMultiArg")
               .doOnNext(System.out::println)
               .as(StepVerifier::create)
               .expectNextCount(1)
               .verifyComplete();

        support.executeToMono("callMultiArg", Collections.singletonMap("val", "123"))
               .as(StepVerifier::create)
               .expectNext("123null")
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

        public void callVoid() {
            System.out.println("callVoid");
        }
    }
}