package com.datappl.Util;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.hadoop.fs.Stat;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import twitter4j.Status;
import com.fasterxml.jackson.databind.JsonNode;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TwitterDeserializer implements Deserializer<Status> {
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Status deserialize(String topic, byte[] bytes) {
        try {
            String json = new String(bytes, StandardCharsets.UTF_8);
            return TwitterObjectFactory.createStatus(json);
        } catch (TwitterException e) {
            return null;
        }
    }

    @Override
    public void close() {

    }
}
