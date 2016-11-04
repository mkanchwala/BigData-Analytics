package com.mkanchwala.ep.kafka.app

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

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

  // Get StreamingContext from checkpoint data or create a new one
  val sparkConf = new SparkConf().setAppName("KafkaDirectOffsetWordCount").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  ssc.checkpoint("/Kafka") // set checkpoint directory

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
      println(s" T=${o.topic};P=${o.partition};From=${o.fromOffset};Until=${o.untilOffset}")

    }
  }

  // Get the lines, split them into words, count the words and print
  val lines = messages.map(_._2)
  val words = lines.flatMap(_.split(" "))
   val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  wordCounts.print()

  words.foreachRDD { rdd =>

    // Get the singleton instance of SQLContext
    val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
    import sqlContext.implicits._

    // Convert RDD[String] to DataFrame
    val wordsDataFrame = rdd.toDF("word")

    // Register as table
    wordsDataFrame.registerTempTable("words")

    // Do word count on DataFrame using SQL and print it
    val wordCountsDataFrame =
      sqlContext.sql("select word, count(*) as total from words group by word")
    wordCountsDataFrame.show()
  }
  
  // Start the computation
  ssc.start()
  ssc.awaitTermination()

}
/*

 T=DomainTest;P=0;From=1;Until=1
 T=DomainTest;P=2;From=2;Until=2
 T=DomainTest;P=1;From=2;Until=2
  
 */
