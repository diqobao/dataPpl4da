package com.datappl.SparkSink

import com.datappl.Util.TwitterDeserializer
import twitter4j.Status;
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._


object SparkConnector2 {

  def createNewSparkServer(name: String, topics: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[TwitterDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

//    val conf = new SparkConf()
//      .setMaster("spark://192.168.1.1:7077")
//      .setAppName(name)
//      .set("spark.rdd.compress","true")
//      .set("spark.storage.memoryFraction", "1")
//      .set("spark.streaming.unpersist", "true")
//
//    val duration = Duration(100)
//
//    val sc = new SparkContext(conf)

    val conf = new SparkConf().setMaster("local[2]").setAppName(name)
    val streamingContext = new StreamingContext(conf, Seconds(1))


//    val streamingContext = new StreamingContext(sc, Seconds(10))
    val stream = KafkaUtils.createDirectStream[Long, Status](
      streamingContext,
      PreferConsistent,
      Subscribe[Long, Status](topics, kafkaParams)
    )

    stream.foreachRDD ((rdd, time) =>
      rdd.map(t => Map(
        "id" -> t.value().getId
      )).saveToEs("twittertest/tweets"))



    val tags = stream.flatMap(_.value().getHashtagEntities())
    val tagCounts = tags.map(tag => (tag, 1)).reduceByKey(_+_)

    tagCounts.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
