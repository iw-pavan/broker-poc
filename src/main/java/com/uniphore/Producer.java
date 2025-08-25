package com.uniphore;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Producer extends Thread {

    private final int messageCount;
    private final Channel channel;

    public Producer(Connection connection, int messageCount) throws IOException {
        this.messageCount = messageCount;
        this.channel = connection.createChannel();
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        ConnectionFactory connectionFactory = RabbitMQUtils.getConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        int threadCount = RabbitMQUtils.THREADS;
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Producer producer = new Producer(connection, RabbitMQUtils.MESSAGES_PER_THREAD);
            es.execute(producer);
        }
        es.shutdown();
        boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("Finished: " + finished);
        es.close();
        connection.close();
        long end = System.currentTimeMillis();
        System.out.println("Producer Time taken: " + (end - start) + " ms");
    }

    @Override
    public void run() {
        for (int i = 0; i < messageCount; i++) {
            String message = "Message " + i + " from " + Thread.currentThread().getName();
            try {
                channel.basicPublish("", "default", null, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
