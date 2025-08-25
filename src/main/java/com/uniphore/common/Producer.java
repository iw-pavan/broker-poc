package com.uniphore.common;

import java.io.Closeable;

public interface Producer extends Closeable {

    void produce(Queue queue, byte[] message, MessagePriority priority);

}
