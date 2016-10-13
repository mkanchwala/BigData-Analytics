package com.mkanchwala.ep.kafka.app;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;
import com.mkanchwala.ep.zookeeper.app.ZKManager;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public final class KafkaOffsetWordCount {
	private static Logger logger = Logger.getLogger(KafkaOffsetWordCount.class);
	private static ZKManager zkClient = new ZKManager("127.0.0.1");

	private static final Pattern SPACE = Pattern.compile(" ");

	@SuppressWarnings("deprecation")
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

		// Hold a reference to the current offset ranges, so it can be used downstream
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);

		JavaPairInputDStream<String, String> messages = null;
		if (zkClient.znode_exists(zkNode) == null) {
			ZKManager.create(zkNode, "".getBytes());
			for(String topic : topics.split(",")){
				ZKManager.create(zkNode + "/" + topic, "".getBytes());
			}
			// Create direct kafka stream with brokers and topics
			messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		} else {

		/*	 class MessageAndMetadataFunction implements Function<MessageAndMetadata<String, String>, Tuple2<String, String>>
		        {

		            @Override
		            public Tuple2<String, String> call(MessageAndMetadata<String, String> v1)
		                    throws Exception {
		                // nothing is printed here
		                System.out.println("topic = " + v1.topic() + ", partition = " + v1.partition());
		                return new Tuple2<String, String>(v1.topic(), v1.message());
		            }

		        }
			 
			// TODO : Zookeeper read offset and start
			messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, String.class, kafkaParams, startOffsetsMap, new MessageAndMetadataFunction());*/
			
			messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		}

		
		// Save : For Offset management in Zookeeper ZNode.
		messages.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
			@Override
			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		}).foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, String> rdd)
					throws IOException, KeeperException, InterruptedException {
				for (OffsetRange o : offsetRanges.get()) {
					String stats = "topic=" + o.topic() + ";partition=" + o.partition() + ";fromOffset=" + o.fromOffset() + ";untilOffset=" + o.untilOffset();
					logger.debug(stats);
					zkClient.update(zkNode + "/" + o.topic(), stats.getBytes());
				}
				return null;
			}
		});

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		
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