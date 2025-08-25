package com.uniphore.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.uniphore.common.Consumer;
import com.uniphore.common.Queue;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class RabbitMQConsumer implements Consumer {

    private final Channel channel;

    public RabbitMQConsumer(Connection connection) throws Exception {
        this.channel = connection.createChannel();
    }

    @Override
    public void consume(Queue queue, java.util.function.Consumer<byte[]> consumer) {
        try {
            channel.basicConsume("default", false, "myConsumerTag",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                Envelope envelope,
                                BasicProperties properties,
                                byte[] body)
                                throws IOException {

                            consumer.accept(body);

                            long deliveryTag = envelope.getDeliveryTag();
                            channel.basicAck(deliveryTag, false);
                        }
                    });
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
