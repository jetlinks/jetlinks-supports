package org.jetlinks.supports.official.types;

import org.jetlinks.core.metadata.types.BooleanType;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class JetLinksBooleanCodecTest {

    @Test
    public void testCodec(){
        JetLinksBooleanCodec codec=new JetLinksBooleanCodec();
        BooleanType type=new BooleanType();
        type.setTrueValue("1");
        type.setFalseValue("0");

        BooleanType newType=codec.decode(new BooleanType(), codec.encode(type));

        Assert.assertEquals(newType.getTrueValue(),"1");
        Assert.assertEquals(newType.getFalseValue(),"0");

    }
}