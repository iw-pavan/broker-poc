package com.uniphore.kafka;

import com.uniphore.common.MessagePriority;
import com.uniphore.common.Producer;
import com.uniphore.common.Queue;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerWrapper implements Producer {

  final KafkaProducer<String, byte[]> kafkaProducer;

  public KafkaProducerWrapper() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("acks", "all");
    props.put("batch.size", 16384);
    props.put("linger.ms", 5);
    props.put("buffer.memory", 33554432);
    kafkaProducer = new KafkaProducer<>(props);
  }

  @Override
  public void produce(Queue queue, byte[] message, MessagePriority priority) {
    String topicName = queue.getName();
    String randomId = java.util.UUID.randomUUID().toString();
    String key = "id-" + randomId;
    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, key, message);
    kafkaProducer.send(record, (RecordMetadata metadata, Exception e) -> {
      if (e != null) {
        e.printStackTrace();
      } else {
        System.out.printf("Sent %s to %s-%d @ offset %d%n", message, metadata.topic(),
            metadata.partition(), metadata.offset());
      }
    });
    //flush should not be after every message in production code
    kafkaProducer.flush();
  }

  @Override
  public void close() {
    kafkaProducer.close();
  }
}
