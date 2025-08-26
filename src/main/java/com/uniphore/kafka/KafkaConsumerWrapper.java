package com.uniphore.kafka;

import com.uniphore.common.Consumer;
import com.uniphore.common.Queue;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerWrapper implements Consumer {

  org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> kafkaConsumer;

  public KafkaConsumerWrapper() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group-1399");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // One message per poll
   // props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    //this time should be more than the processing time of all messages in a batch.
    //should be greater than MAX_POLL_RECORDS_CONFIG * processing_time_per_message in ms
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(15 * 60 * 1000));
    this.kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
  }

  @Override
  public void consume(Queue queue, java.util.function.Consumer<byte[]> consumer) {
    String topicName = queue.getName();
    this.kafkaConsumer.subscribe(java.util.Collections.singletonList(topicName));
    while (true) {
      //Need Some interruption mechanism to exit this loop and close the consumer
      ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofSeconds(1));

      // Track latest offsets per partition
      Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

      for (ConsumerRecord<String, byte[]> record : records) {
        System.out.printf("Partition=%d, Offset=%d %n",
            record.partition(), record.offset());
        // Your business logic
        consumer.accept(record.value());
        // Remember the last offset for this partition
        offsetsToCommit.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
        );
      }
      if (!offsetsToCommit.isEmpty()) {
        kafkaConsumer.commitSync(offsetsToCommit);
        offsetsToCommit.forEach((tp, offsetMeta) ->
            System.out.printf("Committed offset %d for partition %d %n",
                offsetMeta.offset(), tp.partition()));
      }
    }
  }

  @Override
  public void close() {
    kafkaConsumer.close();
  }
}
