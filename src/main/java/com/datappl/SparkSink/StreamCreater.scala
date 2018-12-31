package com.datappl.SparkSink

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import twitter4j.Status

object StreamCreater {

  def createStream(streamingContext: StreamingContext, topics: Array[String], kafkaParams: Map[String, Object]):InputDStream[ConsumerRecord[Long,Status]]= {

    val stream = KafkaUtils.createDirectStream[Long, Status](
      streamingContext,
      PreferConsistent,
      Subscribe[Long, Status](topics, kafkaParams)
    )
    stream
  }
}
