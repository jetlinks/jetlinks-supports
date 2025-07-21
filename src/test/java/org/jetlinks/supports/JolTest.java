package org.jetlinks.supports;

import com.google.common.collect.Maps;
import org.jetlinks.core.lang.SharedPathString;
import org.openjdk.jol.info.ClassLayout;

import java.util.Collections;

public class JolTest {
    public static void main(String[] args) {
        ClassLayout layout = ClassLayout.parseClass(
            SharedPathString.class
        );

        System.out.println(layout.headerSize()+layout.instanceSize());
        System.out.println(layout.toPrintable());
    }
}
