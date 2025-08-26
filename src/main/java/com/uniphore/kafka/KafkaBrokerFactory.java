package com.uniphore.kafka;
import com.uniphore.common.BrokerFactory;
import com.uniphore.common.Consumer;
import com.uniphore.common.Producer;
import com.uniphore.common.Queue;

public class KafkaBrokerFactory implements BrokerFactory {

  @Override
  public Queue getQueue() {
    return ()-> "batch-topic-1m-4p-byte-array-3";
  }

  @Override
  public Queue getDelayedQueue() {
    return ()->"batch-topic-1m-4p-byte-array-3-delayed";
  }

  @Override
  public Consumer getConsumer() {
    return new KafkaConsumerWrapper();
  }

  @Override
  public Producer getProducer() {
    return new KafkaProducerWrapper();
  }

  @Override
  public void close() {

  }
}
