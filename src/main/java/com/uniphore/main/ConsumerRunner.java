package com.uniphore.main;

import com.uniphore.common.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerRunner extends Runner {

    public static void main(String[] args) throws InterruptedException {
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
        System.out.println("Consumer Time taken: " + (end - start) + " ms");
    }

}
