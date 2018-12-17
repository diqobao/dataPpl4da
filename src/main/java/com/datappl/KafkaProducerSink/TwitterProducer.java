package com.datappl.KafkaProducerSink;

import java.util.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import twitter4j.*;

//import com.datappl.config.props.KafkaProperties;

public class TwitterProducer {

    final private Producer<String, String> producer;

    public TwitterProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void twitterProducerStart() {
        TwitterFactory tf = new TwitterFactory();
        Twitter twitter = tf.getInstance();

        int count = 0;
        StatusListener listener = new StatusListener() {

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice arg) {
//                System.out.println(status.getText());
            }
            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
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
                producer.send(new ProducerRecord<String, String>(Long.toString(status.getUser().getId()), Long.toString(status.getId()), status.getText()));
                System.out.println("Sent:" + status.getUser().getId());
                producer.send(new ProducerRecord<String, String>("test", Long.toString(status.getId()), status.getText()));
                System.out.println("Sent:" + status.getText());
                if(status.getPlace() != null) producer.send(new ProducerRecord<String, String>(status.getPlace().getCountryCode(), Long.toString(status.getId()), status.getText()));
                else producer.send(new ProducerRecord<String, String>("NullSpace", Long.toString(status.getId()), status.getText()));

                if(status.getPlace() != null) System.out.println(status.getPlace().getCountryCode());

            }
            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }
        };


        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

        twitterStream.addListener(listener);

        twitterStream.sample();

    }

    public static void main(String[] args) throws Exception {
//        ConfigurationBuilder cb = new ConfigurationBuilder();
//        cb.setDebugEnabled(true)
//                .setOAuthConsumerKey(oauth.consumerKey)
//                .setOAuthConsumerSecret("your consumer secret")
//                .setOAuthAccessToken("your access token")
//                .setOAuthAccessTokenSecret("your access token secret");
//        TwitterFactory tf = new TwitterFactory(cb.build());


        TwitterProducer producer = new TwitterProducer();
        producer.twitterProducerStart();


    }

}
