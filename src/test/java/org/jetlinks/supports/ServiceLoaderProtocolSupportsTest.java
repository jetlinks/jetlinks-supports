package org.jetlinks.supports;

import org.jetlinks.supports.protocol.ServiceLoaderProtocolSupports;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ServiceLoaderProtocolSupportsTest {
    private ServiceLoaderProtocolSupports supports = new ServiceLoaderProtocolSupports();

    @Before
    public void init() {

        supports.init();
    }

    @Test
    public void test() {
        Assert.assertTrue(supports.isSupport("jetlinks.v1.0"));
    }

}