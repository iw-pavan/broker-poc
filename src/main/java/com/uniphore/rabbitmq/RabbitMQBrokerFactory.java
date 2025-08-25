package com.uniphore.rabbitmq;

import com.rabbitmq.client.Connection;
import com.uniphore.common.BrokerFactory;
import com.uniphore.common.Consumer;
import com.uniphore.common.Producer;
import com.uniphore.common.Queue;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQBrokerFactory implements BrokerFactory {

    private final Connection connection;

    public RabbitMQBrokerFactory() {
        try {
            this.connection = RabbitMQUtils.getConnectionFactory().newConnection();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Queue getQueue() {
        return () -> "default";
    }

    @Override
    public Queue getDelayedQueue() {
        return () -> "default-delayed";
    }

    @Override
    public Consumer getConsumer() {
        try {
            return new RabbitMQConsumer(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Producer getProducer() {
        try {
            return new RabbitMQProducer(connection);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
