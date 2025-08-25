package com.uniphore.common;

import java.io.Closeable;

public interface BrokerFactory extends Closeable {

    Queue getQueue();

    Queue getDelayedQueue();

    Consumer getConsumer();

    Producer getProducer();

}
