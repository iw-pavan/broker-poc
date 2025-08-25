package com.uniphore.main;

import com.uniphore.common.BrokerFactory;
import com.uniphore.rabbitmq.RabbitMQBrokerFactory;

public class Runner {

    protected static final int THREADS = 4;
    protected static final int MESSAGES_PER_THREAD = 1000;
    private static final BrokerFactory BROKER_FACTORY = new RabbitMQBrokerFactory();

    protected static BrokerFactory getBrokerFactory() {
        return BROKER_FACTORY;
    }

}
