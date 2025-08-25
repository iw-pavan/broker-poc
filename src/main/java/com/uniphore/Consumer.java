package com.uniphore;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer extends Thread {

    private static final AtomicInteger COUNTER = new AtomicInteger(RabbitMQUtils.MESSAGES_PER_THREAD * RabbitMQUtils.THREADS);

    private final Channel channel;

    public Consumer (Connection connection) throws Exception {
        this.channel = connection.createChannel();
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        ConnectionFactory connectionFactory = RabbitMQUtils.getConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        int threadCount = RabbitMQUtils.THREADS;
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Consumer producer = new Consumer(connection);
            producer.setName("Producer-" + i);
            es.execute(producer);
        }
        es.shutdown();
        boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("Finished: " + finished);
        es.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("Consumer Time taken: " + (end - start) + " ms");
    }

    @Override
    public void run() {
        try {
            String c = channel.basicConsume("default", false, "myConsumerTag",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                Envelope envelope,
                                BasicProperties properties,
                                byte[] body)
                                throws IOException {

                            System.out.println("Received '" + new String(body) + "' from "
                                    + Thread.currentThread().getName());

                            long deliveryTag = envelope.getDeliveryTag();
                            channel.basicAck(deliveryTag, false);
                            COUNTER.decrementAndGet();
                        }
                    });
            while (COUNTER.get() > 0) {
                Thread.sleep(10);
            }
            channel.basicCancel(c);
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
