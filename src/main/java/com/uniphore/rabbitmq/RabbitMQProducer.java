package com.uniphore.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.uniphore.common.Message;
import com.uniphore.common.Producer;
import com.uniphore.common.Queue;
import com.uniphore.common.Serde;
import java.io.IOException;

public class RabbitMQProducer implements Producer {

    private final Channel channel;

    public RabbitMQProducer(Connection connection) throws IOException {
        this.channel = connection.createChannel();
    }

    @Override
    public void produce(Queue queue, byte[] message) {
        try {
            channel.basicPublish("", "default", null, message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
