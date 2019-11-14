package org.jetlinks.supports.official.cipher;

import org.hswebframework.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class CiphersTest {


    @Test
    public void testAes() {
        String key = RandomUtil.randomChar(16);
        System.out.println(key);
        String enc = Ciphers.AES.encryptBase64("test", key);

        System.out.println(enc);
        String dec = new String(Ciphers.AES.decryptBase64(enc, key));
        System.out.println(dec);
        Assert.assertEquals("test", dec);

    }

}