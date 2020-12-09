package org.jetlinks.supports.cluster.event;

import io.rsocket.util.ByteBufPayload;
import org.jetlinks.core.event.TopicPayload;
import org.junit.Test;

import static org.junit.Assert.*;

public class RSocketPayloadTest {


    @Test
    public void test(){
        RSocketPayload payload = RSocketPayload.of(ByteBufPayload.create("hello"));

        payload.retain();

        assertEquals(payload.refCnt(),2);

        payload.release();
        assertEquals(payload.refCnt(),1);

        payload.release();
        assertEquals(payload.refCnt(),0);
    }

    @Test
    public void testTopic(){
        RSocketPayload payload = RSocketPayload.of(ByteBufPayload.create("hello"));
        TopicPayload topicPayload = TopicPayload.of("/test",payload);

        topicPayload.retain();

        assertEquals(topicPayload.refCnt(),2);

        topicPayload.release();
        assertEquals(topicPayload.refCnt(),1);

        topicPayload.release();
        assertEquals(topicPayload.refCnt(),0);

        assertEquals(payload.refCnt(),0);
    }
}