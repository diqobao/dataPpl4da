package com.datappl.KafkaProducerSink;

import com.datappl.config.props.*;
import java.util.*;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import scala.collection.mutable.HashEntry$class;
import twitter4j.*;
import twitter4j.TwitterObjectFactory;

import java.io.ObjectOutputStream;

//import com.datappl.config.props.KafkaProperties;

public class TwitterProducer {

    final private Producer<Long, String> producer;
    final private Producer<Long, String> producer2;
    final private Producer<Integer, String> excProducer;
    final private Producer<Long, Long> delProducer;
    final private KafkaProperties KafkaProps;

    public TwitterProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        producer2 = new KafkaProducer<>(props);

        KafkaProps = new KafkaProperties();
        delProducer = new KafkaProducer<>(KafkaProps.delProp());
        excProducer = new KafkaProducer<>(KafkaProps.excProp());
    }

    public void twitterProducerStart() {
        TwitterFactory tf = new TwitterFactory();
        Twitter twitter = tf.getInstance();

//        int count = 0;
        StatusListener listener = new StatusListener() {

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
                excProducer.send(new ProducerRecord<Integer, String>("exc", e.hashCode(),e.getMessage()));
                System.out.println(e.getMessage());
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice arg) {
//                System.out.println(status.getText());
                delProducer.send(new ProducerRecord<Long, Long>("del", arg.getStatusId(), arg.getUserId()));
                System.out.println(arg.getUserId());
            }
            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                delProducer.send(new ProducerRecord<Long, Long>("geoDel", upToStatusId, userId));
            }
            @Override
            public void onStallWarning(StallWarning warning) {
            }
            @Override
            public void onStatus(Status status) {
//                if (status == null) {
//                    Thread.sleep(100);
//                }else {
//                    for(HashtagEntity hashtage : status.getHashtagEntities()) {
//                        System.out.println("Hashtag: " + hashtage.getText());
//                        producer.send(new ProducerRecord<String, String>(
//                                top-icName, Integer.toString(j++), hashtage.getText()));
//                    }
//                }
//                String statusJson = TwitterObjectFactory.getRawJSON(status);
//                JSONObject JSON_complete = new JSONObject(statusJson);
//                String languageTweet = JSON_complete.getString("text");
//                System.out.println(languageTweet);

                //Gson gson = new Gson();
                //String json = gson.toJson( status );
                String json = TwitterObjectFactory.getRawJSON(status);
                producer.send(new ProducerRecord<Long, String>("test", status.getId(), json));
                String space = "NullPlace";
                if(status.getPlace() != null) space = status.getPlace().getCountryCode();
                System.out.println(space);
                producer2.send(new ProducerRecord<Long, String>(space, status.getId(), json));
//                System.out.println("Sent: " + space);
//                if(status.getPlace() != null) producer.send(new ProducerRecord<String, String>(status.getPlace().getCountryCode(), Long.toString(status.getId()), status.getText()));
//                else producer.send(new ProducerRecord<String, String>("NullSpace", Long.toString(status.getId()), status.getText()));

//                if(status.getPlace() != null) System.out.println(status.getPlace().getCountryCode());

            }
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }
        };


        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

        twitterStream.addListener(listener);

        twitterStream.sample();

    }

}
