package com.uniphore.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.uniphore.common.Message;
import com.uniphore.common.MessagePriority;
import com.uniphore.common.Producer;
import com.uniphore.common.Queue;
import com.uniphore.common.Serde;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducer implements Producer {

    private final Channel channel;

    public RabbitMQProducer(Connection connection) throws IOException {
        this.channel = connection.createChannel();
    }

    @Override
    public void produce(Queue queue, byte[] message, MessagePriority priority) {
        try {
            channel.basicPublish("", "default", null, message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            channel.close();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
