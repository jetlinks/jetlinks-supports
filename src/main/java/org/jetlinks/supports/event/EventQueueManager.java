package org.jetlinks.supports.event;

public interface EventQueueManager {


    EventQueue getQueue(String topic, String subscriber);


}
