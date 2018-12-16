package com.datappl.KafkaProducerSink;

import java.util.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

public class producer {

    public static void main(String[] args) {
        String topicName = "topic1";
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer0 = new KafkaProducer<>(props);

//        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < 100; i++) {
                long runtime = new Date().getTime();
                String ip = "192.168.1." + i;
                String msg = runtime + "#" + ip + "#" + "aaa";

                producer0.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), msg));
                System.out.println("Sent:" + msg);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            producer0.close();
        }


    }

}
