package com.datappl;

import com.datappl.KafkaProducerSink.TwitterProducer;
import com.datappl.KafkaConsumerSink.Consumer;

public class KafkaTest {
    public static void main(String[] args) {
//        ConfigurationBuilder cb = new ConfigurationBuilder();
//        cb.setDebugEnabled(true)
//                .setOAuthConsumerKey(oauth.consumerKey)
//                .setOAuthConsumerSecret("your consumer secret")
//                .setOAuthAccessToken("your access token")
//                .setOAuthAccessTokenSecret("your access token secret");
//        TwitterFactory tf = new TwitterFactory(cb.build());

        TwitterProducer producer = new TwitterProducer();
        producer.twitterProducerStart();
        Consumer consumer = new Consumer(new String[]{"test"});
        consumer.consumerStart();

    }
}
