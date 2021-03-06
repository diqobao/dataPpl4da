package com.datappl.SparkSink

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import twitter4j.Status
import org.elasticsearch.spark._

object SparkProcessor {

  def statusProcessor(stream: InputDStream[ConsumerRecord[Long,Status]]): DStream[(String, Int)] = {
    stream.foreachRDD ((rdd, time) =>
      rdd.map(t => Map(
        "id" -> t.value().getId,
        "user" -> t.value().getUser.getId,
        "create_at" -> t.value().getCreatedAt,
        "hashtags" -> t.value().getHashtagEntities.map(_.getText),
        "taglen" -> t.value().getHashtagEntities.length,
        "text" -> t.value().getText
      )).saveToEs("twittertest/tweets"))



    val tags = stream.flatMap(_.value().getHashtagEntities())
    val tagCounts = tags.map(tag => (tag.getText, 1)).reduceByKey(_+_)

    tagCounts
  }

  def delProcessor(stream: InputDStream[ConsumerRecord[Long,Long]]): DStream[(Long, Int)] = {
    stream.foreachRDD((rdd, time) =>
    rdd.map(t => Map(
      "sid" -> t.key(),
      "uid" -> t.value()
    )).saveToEs("twitterdel/deletions"))
    val userCounts = stream.map(t => (t.value(), 1)).reduceByKey(_+_)
    userCounts
  }

  def excProcessor(stream: InputDStream[ConsumerRecord[Int,Int]]): DStream[(Int, Int)] = {
    val excCounts = stream.map(t => (t.key(), 1)).reduceByKey(_+_)
    excCounts
  }
}
