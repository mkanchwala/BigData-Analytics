package com.mkanchwala.ep.kafka.app;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;
import com.mkanchwala.ep.zookeeper.app.ZKManager;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. 
 * 
 * Usage:
 * 
 * KafkaOffsetWordCount <brokers> <topics>
 *  
 * where, 
 * 
 * <brokers> is a list of one or more Kafka brokers 
 * <topics> is a list of one or more kafka topics to consume from
 * <zkNode> is the base path for zookeeper base znode.
 *
 * Example: $ KafkaOffsetWordCount 127.0.0.1:9092 test,test2 Spark 
 */
public final class KafkaOffsetWordCount {
	private static Logger logger = Logger.getLogger(KafkaOffsetWordCount.class);
	private static ZKManager zkClient = new ZKManager("127.0.0.1");

	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings({ "serial" })
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n"
					+ "  <znode-path> is a znode location to store the offsets \n\n");
			System.exit(1);
		}

		String brokers = args[0];
		final String topics = args[1];
		final String zkNode = "/" + args[2];

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("KafkaOffsetWordCount").setMaster("local[4]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		// Hold a reference to the current offset ranges, so it can be used
		// downstream
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("auto.offset.reset", "largest");

		JavaDStream<String> lines = null;

		if (zkClient.znode_exists(zkNode) == null) {
			logger.debug("Taking Fresh Stream .... ");
			zkClient.create(zkNode, "".getBytes());
			for (String topic : topics.split(",")) {
				zkClient.create(zkNode + "/" + topic, "".getBytes());
			}
			// Create direct kafka stream with brokers and topics
			lines = KafkaUtils
					.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
							kafkaParams, topicsSet)
					.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
						@Override
						public JavaRDD<String> call(JavaPairRDD<String, String> pairRdd) throws Exception {
							JavaRDD<String> rdd = pairRdd.map(new Function<Tuple2<String, String>, String>() {
								@Override
								public String call(Tuple2<String, String> arg0) throws Exception {
									return arg0._2;
								}
							});
							zkClient.saveOffset(((HasOffsetRanges) rdd.rdd()).offsetRanges(), zkNode);
							return rdd;
						}
					});
		} else {

			logger.debug("Resuming operations .... ");
			Map<TopicAndPartition, Long> startOffsetsMap = zkClient.findOffsetRange(zkNode);
			logger.debug("Map Size : " + startOffsetsMap.size());

			lines = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
					StringDecoder.class, String.class, kafkaParams, startOffsetsMap,
					new Function<MessageAndMetadata<String, String>, String>() {
						@Override
						public String call(MessageAndMetadata<String, String> msgAndMd) {
							return msgAndMd.message();
						}
					}).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

						@Override
						public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
							zkClient.saveOffset(((HasOffsetRanges) rdd.rdd()).offsetRanges(), zkNode);
							return rdd;
						}
					});
		}

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				return Lists.newArrayList(Arrays.asList(SPACE.split(x)).iterator());
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		wordCounts.print();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}