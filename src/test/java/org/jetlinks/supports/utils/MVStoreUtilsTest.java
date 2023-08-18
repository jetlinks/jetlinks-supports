package org.jetlinks.supports.utils;

import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.Assert.*;

public class MVStoreUtilsTest {


    @Test
    public void test() {
        AtomicReference<MVStoreUtils.MVStoreOpening.Action> actRef = new AtomicReference<>();

        MVStoreUtils.addHook((act, opening) -> {
            actRef.set(act);
            System.out.println(act+","+opening.getError());

        });

        MVStore store = MVStoreUtils
                .open(new File("./target/test.db"),
                      "测试",
                      Function.identity());
        assertEquals(MVStoreUtils.MVStoreOpening.Action.success, actRef.get());

        MVStoreUtils
                .open(new File("./target/test.db"),
                      "测试",
                      Function.identity());



    }

}