package com.uniphore.main;

import com.uniphore.common.Consumer;
import com.uniphore.common.Message;
import com.uniphore.common.Queue;
import com.uniphore.common.Serde;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerRunner extends Runner {

    private static final AtomicInteger COUNTER = new AtomicInteger(THREADS * MESSAGES_PER_THREAD);

    public static void main(String[] args) throws InterruptedException, IOException {
        long start = System.currentTimeMillis();

        int threadCount = THREADS;
        Queue queue = getBrokerFactory().getQueue();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Runnable runnable = () -> {
                Consumer consumer = getBrokerFactory().getConsumer();
                consumer.consume(queue, body -> {
                    // Process the message
                    if (body != null && body.length > 0) {
                        Message message = Serde.deserialize(body);
                        System.out.println("Received: " + message.getMessageBody() + " by " + Thread.currentThread().getName());
                    }
                    COUNTER.decrementAndGet();
                });
                while (COUNTER.get() > 0) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                try {
                    consumer.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            es.execute(runnable);

        }
        es.shutdown();
        boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("Finished: " + finished);
        es.close();

        long end = System.currentTimeMillis();
        System.out.println("Consumer Time taken: " + (end - start) + " ms");
        getBrokerFactory().close();
    }

}
