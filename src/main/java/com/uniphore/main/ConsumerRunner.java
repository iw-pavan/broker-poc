package com.uniphore.main;

import com.uniphore.common.Consumer;
import com.uniphore.common.Message;
import com.uniphore.common.Queue;
import com.uniphore.common.Serde;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

        List<Consumer> consumers = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Consumer consumer = getBrokerFactory().getConsumer();
            consumers.add(consumer);
            es.submit(() -> consumer.consume(queue, body -> {
                if (body != null && body.length > 0) {
                    Message message = Serde.deserialize(body);
                    System.out.printf("Received: %s by %s%n",
                        message.getMessageBody(), Thread.currentThread().getName());
                }
                COUNTER.decrementAndGet();
            }));
        }

        // Wait until all messages processed
        while (COUNTER.get() > 0) {
            Thread.sleep(50);
        }
        // Trigger consumer shutdown
        for (Consumer consumer : consumers) {
            consumer.close();
        }

        es.shutdown();
        boolean finished = es.awaitTermination(5, TimeUnit.MINUTES);
        System.out.println("Finished: " + finished);

        long end = System.currentTimeMillis();
        System.out.println("Consumer Time taken: " + (end - start) + " ms");

        getBrokerFactory().close();
    }

}
