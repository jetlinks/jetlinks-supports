package org.jetlinks.supports.protocol.codec;

public interface BinaryProtocolCodecBuilder {


    interface PropertyMatchSpec {

        PropertyMatchSpec matcher(CodecPredicate predicate);


    }

}
