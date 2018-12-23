package com.datappl.KafkaConsumerSink;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import twitter4j.Status;
import com.datappl.Util.TwitterDeserializer;

import java.util.*;

public class Consumer {

    private String[] topicName;
    private Properties props;
    private KafkaConsumer<Long, Status> consumer;

    public Consumer(String[] _topicName) {
        topicName = _topicName;
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "com.datappl.Util.TwitterDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

    }

    public void consumerStart() {
        while (true) {
            ConsumerRecords<Long, Status> records = consumer.poll(100);
            for (ConsumerRecord<Long, Status> record : records)
//                System.out.println(record.value().getPlace().getCountryCode());
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value().getHashtagEntities().length);
        }
    }
}
