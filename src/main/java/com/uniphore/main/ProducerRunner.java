package com.uniphore.main;

import com.uniphore.common.Message;
import com.uniphore.common.MessagePriority;
import com.uniphore.common.Producer;
import com.uniphore.common.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerRunner extends Runner {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        int threadCount = THREADS;
        Queue queue = getBrokerFactory().getQueue();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Runnable producerRunnable = getRunnable(i, queue);
            es.execute(producerRunnable);
        }
        es.shutdown();
        boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("Finished: " + finished);
        es.close();

        long end = System.currentTimeMillis();
        System.out.println("Producer Time taken: " + (end - start) + " ms");
    }

    private static Runnable getRunnable(final int i, Queue queue) {
        return  () -> {
            Producer producer = getBrokerFactory().getProducer();
            for (int j = 0 ; j < MESSAGES_PER_THREAD; j++) {
                String message = "Message-" + j + " from " + Thread.currentThread().getName();
                try {
                    Message m = new Message(i * j, message);
                    producer.produce(queue, message.getBytes(), MessagePriority.NORMAL);
                    if (j % 100 == 0) {
                        System.out.println("Produced: " + message);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }

}
