package com.uniphore.common;

public interface BrokerFactory {

    Queue getQueue();

    Queue getDelayedQueue();

    Consumer getConsumer();

    Producer getProducer();

}
