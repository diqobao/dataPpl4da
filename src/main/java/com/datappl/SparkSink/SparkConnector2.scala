package com.datappl.SparkSink

import com.datappl.Util.TwitterDeserializer
import twitter4j.Status
import org.apache.kafka.common.serialization.{IntegerDeserializer, LongDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import com.datappl.SparkSink.StreamCreater
import com.datappl.SparkSink.SparkProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream


class SparkConnector2(private var name: String, private var topics: Array[String], private var _type: String) {

//  var kafkaParams: Map[String, Object]
//  var streamingContext: StreamingContext
//  var topics: Array[String]
//  var _type: String
//  var name: String
  var kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "group1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
//    "key.deserializer" -> classOf[LongDeserializer],
//    "value.deserializer" -> classOf[LongDeserializer]
  )
  if(_type.equals("status")) {
//    kafkaParams.updated("key.deserializer", classOf[LongDeserializer])
//    kafkaParams.updated("value.deserializer", classOf[TwitterDeserializer])
    kafkaParams = kafkaParams + ("key.deserializer" -> classOf[LongDeserializer])
    kafkaParams = kafkaParams + ("value.deserializer" -> classOf[TwitterDeserializer])
  }else if(_type.equals("delete")) {
//    kafkaParams.updated("key.deserializer", classOf[LongDeserializer])
//    kafkaParams.updated("value.deserializer", classOf[LongDeserializer])
    kafkaParams = kafkaParams + ("key.deserializer" -> classOf[LongDeserializer])
    kafkaParams = kafkaParams + ("value.deserializer" -> classOf[LongDeserializer])
  }else if(_type.equals("exception")) {
//    kafkaParams.updated("key.deserializer", classOf[IntegerDeserializer])
//    kafkaParams.updated("value.deserializer", classOf[IntegerDeserializer])
    kafkaParams = kafkaParams + ("key.deserializer" -> classOf[IntegerDeserializer])
    kafkaParams = kafkaParams + ("value.deserializer" -> classOf[IntegerDeserializer])
  }


  def createNewSparkServer(): Unit = {
    //    val kafkaParams = Map[String, Object](
    //      "bootstrap.servers" -> "localhost:9092",
    //      "group.id" -> "group1",
    //      "auto.offset.reset" -> "latest",
    //      "enable.auto.commit" -> (false: java.lang.Boolean)
    //    )
    //
    //    if(_type.equals("status")) {
    //      kafkaParams.updated("key.deserializer", classOf[LongDeserializer])
    //      kafkaParams.updated("value.deserializer", classOf[TwitterDeserializer])
    //    }else if(_type.equals("delete")) {
    //      kafkaParams.updated("key.deserializer", classOf[LongDeserializer])
    //      kafkaParams.updated("value.deserializer", classOf[LongDeserializer])
    //    }else if(_type.equals("exception")) {
    //      kafkaParams.updated("key.deserializer", classOf[IntegerDeserializer])
    //      kafkaParams.updated("value.deserializer", classOf[IntegerDeserializer])
    //    }

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
    //    val stream = KafkaUtils.createDirectStream[Long, Status](
    //      streamingContext,
    //      PreferConsistent,
    //      Subscribe[Long, Status](topics, kafkaParams)
    //    )

//    val stream = _type match {
//      case "status" => StreamCreater.createStream(streamingContext, topics, kafkaParams)
//    }
    if (_type.equals("status")) {
      val stream = StreamCreater.createStreamStatus(streamingContext, topics, kafkaParams)
      val res = SparkProcessor.statusProcessor(stream)
      res.print()
    } else if (_type.equals("delete")) {
      val stream = StreamCreater.createStreamDel(streamingContext, topics, kafkaParams)
      val res = SparkProcessor.delProcessor(stream)
      res.print()
    } else if (_type.equals("exception")) {
      val stream = StreamCreater.createStreamExc(streamingContext, topics, kafkaParams)
      val res = SparkProcessor.excProcessor(stream)
      res.print()
    }


//    stream.foreachRDD ((rdd, time) =>
//      rdd.map(t => Map(
//        "id" -> t.value().getId,
//        "user" -> t.value().getUser.getId,
//        "create_at" -> t.value().getCreatedAt,
//        "hashtags" -> t.value().getHashtagEntities.map(_.getText),
//        "taglen" -> t.value().getHashtagEntities.length,
//        "text" -> t.value().getText
//      )).saveToEs("twittertest/tweets"))
//
//
//
//    val tags = stream.flatMap(_.value().getHashtagEntities())
//    val tagCounts = tags.map(tag => (tag, 1)).reduceByKey(_+_)
//    val res = _type match {
//      case "status" => SparkProcessor.statusProcessor(stream)
//      case "delete" => SparkProcessor.statusProcessor(stream)
//    }
//    res.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
//
//  def SparkProcessorSelector(): InputDStream[ConsumerRecord[Long,Status]] = {
//    SparkProcessor.processor0(stream)
//  }

}
