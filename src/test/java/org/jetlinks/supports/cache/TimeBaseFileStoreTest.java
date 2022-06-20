package org.jetlinks.supports.cache;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimeBaseFileStoreTest {

    TimeBaseFileStore<Integer> data = TimeBaseFileStore.open("./target/data", 10);

    @After
    public void shutdown() {
        // data.clear();
        data.dispose();
    }

    @Test
    public void benchmark() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            for (int i1 = 0; i1 < 10000; i1++) {
                for (int i2 = 0; i2 < 10; i2++) {
                    data.set("prod", "device" + i1 + ":temp" + i2, i1 + i2, i2);
                }
            }
        }

        System.out.println("write:" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            for (int i1 = 0; i1 < 10000; i1++) {
                for (int i2 = 0; i2 < 10; i2++) {
                    data.get("prod", "device" + i1 + ":temp" + i2, i1 + i2);
                }
            }
        }
        System.out.println("read:" + (System.currentTimeMillis() - start));
    }

    @Test
    public void test() {

        data.set("device1", "temp1", 1, 1);
        data.set("device1", "temp1", 3, 3);
        data.set("device1", "temp1", 5, 5);
        data.set("device1", "temp1", 7, 7);


        assertEquals(new Integer(3), data.get("device1", "temp1", 4));

    }
}