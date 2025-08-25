package com.uniphore.common;

public interface Producer {

    void produce(Queue queue, byte[] message, MessagePriority priority);

}
