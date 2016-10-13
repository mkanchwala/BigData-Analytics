package com.mkanchwala.ep.kafka.app

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object KafkaDirectOffsetWordCount extends App {
  if (args.length < 2) {
    System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
    System.exit(1)
  }

  val Array(brokers, topics) = args

  // Create context with 2 second batch interval
  val sparkConf = new SparkConf().setAppName("KafkaDirectOffsetWordCount").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  // Create direct kafka stream with brokers and topics
  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)

  // Hold a reference to the current offset ranges, so it can be used downstream
  var offsetRanges = Array[OffsetRange]()

  messages.transform { rdd =>
    offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd
  }.foreachRDD { rdd =>
    for (o <- offsetRanges) {
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      
    }
  }
  
  // Get the lines, split them into words, count the words and print
  val lines = messages.map(_._2)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  wordCounts.print()

  // Start the computation
  ssc.start()
  ssc.awaitTermination()
}