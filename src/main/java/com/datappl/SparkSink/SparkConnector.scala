package com.datappl.SparkSink

import javafx.css.converter.PaintConverter.SequenceConverter
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object SparkConnector {

  def createNewSparkServer(name: String, topics: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
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
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val words = stream.flatMap(_.value().split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_+_)
    //    val offsetRanges = Array(
//      // topic, partition, inclusive starting offset, exclusive ending offset
//      OffsetRange("test", 0, 0, 100),
//      OffsetRange("test", 1, 0, 100)
//    )

//    val rdd = KafkaUtils.createRDD[String, String](streamingContext, kafkaParams, offsetRanges, PreferConsistent)

//    stream.map(record => (record.key, record.value.length()))
//
//    stream.foreachRDD { rdd =>
//      rdd.foreach(record => record.value().split(" ").foreach(println))
//    }



//    stream.print()
    wordCounts.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
