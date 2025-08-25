package com.uniphore.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQUtils {

    public static ConnectionFactory getConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("infoworks");
        factory.setPassword("IN11**rk");
        factory.setVirtualHost("infoworks_host");
        return factory;
    }

}
