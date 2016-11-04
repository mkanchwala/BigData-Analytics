package com.mkanchwala.ep.kafka.app;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import com.mkanchwala.ep.hbase.dao.MyKryoRegistrator;
import com.mkanchwala.ep.hbase.dao.WordCountDAO;
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
 * KafkaSlidingWindowWordCount <brokers> <topics>
 * 
 * where,
 * 
 * <brokers> is a list of one or more Kafka brokers <topics> is a list of one or
 * more kafka topics to consume from <zkNode> is the base path for zookeeper
 * base znode.
 *
 * Example: $ KafkaOffsetWordCount 127.0.0.1:9092 test,test2 Spark
 */
public final class KafkaSlidingWindowWordCount {
	private static Logger logger = Logger.getLogger(KafkaSlidingWindowWordCount.class);
	private static Configuration conf;
	static Job newAPIJobConfiguration;
	
	@SuppressWarnings("unused")
	private static HTable table; 
	private static ZKManager zkClient;

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
		final String zkNode = "/" + args[2] + "/Znode";
		zkClient = new ZKManager("ubuntu-02", args[2]);

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("KafkaOffsetWordCount").setMaster("local[4]");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.kryo.registrator", Put.class.getName());
		
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
							zkClient.saveOffset(((HasOffsetRanges) pairRdd.rdd()).offsetRanges(), zkNode);

							JavaRDD<String> rdd = pairRdd.map(new Function<Tuple2<String, String>, String>() {
								@Override
								public String call(Tuple2<String, String> message) throws Exception {
									return message._2;
								}
							});
							return rdd;
						}
					});
		} else {

			logger.debug("Resuming operations .... ");
			Map<TopicAndPartition, Long> startOffsetsMap = zkClient.findOffsetRange(zkNode);

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
				return Lists.newArrayList(SPACE.split(x));
			}
		});

		// Convert RDDs of the words DStream to DataFrame and run SQL query
		words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());
				WordCountDAO wordCountDao = new WordCountDAO();

				// Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
				JavaRDD<JavaRecord> rowRDD = rdd.map(new Function<String, JavaRecord>() {
					public JavaRecord call(String word) {
						JavaRecord record = new JavaRecord();
						record.setWord(word);
						return record;
					}
				});
				DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, JavaRecord.class);

				// Register as table
				wordsDataFrame.registerTempTable("words");

				// Do word count on table using SQL and print it
				DataFrame wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word");
				
				if(!wordCountsDataFrame.javaRDD().isEmpty()){
					// Write here in HBase
					initializeHbaseConfiguration();
					wordCountDao.writeRowNewHadoopAPI(wordCountsDataFrame.javaRDD(), conf);
				}
				
				wordCountsDataFrame.show();
			}
		});
		
		/*words.foreachRDD(new Function<JavaRDD<String>, Void>() {
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());

				// Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
				JavaRDD<JavaRecord> rowRDD = rdd.map(new Function<String, JavaRecord>() {
					public JavaRecord call(String word) {
						JavaRecord record = new JavaRecord();
						record.setWord(word);
						return record;
					}
				});
				DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, JavaRecord.class);

				// Register as table
				wordsDataFrame.registerTempTable("words");

				// Do word count on table using SQL and print it
				DataFrame wordCountsDataFrame = sqlContext
						.sql("select word, count(*) as total from words group by word");
				wordCountsDataFrame.show();
				return null;
			}
		});*/
		
		jssc.start();
		jssc.awaitTermination();
	}

	@SuppressWarnings("deprecation")
	public static Configuration initializeHbaseConfiguration() throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
		String hbaseTableName = "WordCount";
		if (conf == null) {
			conf = HBaseConfiguration.create();
			conf.set(HConstants.ZOOKEEPER_QUORUM, "ubuntu-02");
			conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
			HBaseAdmin.checkHBaseAvailable(conf);
			table = new HTable(conf, hbaseTableName);
			logger.info("Hbase is in Running Mode!");
			conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, hbaseTableName);
			try {
				newAPIJobConfiguration = Job.getInstance(conf);
				newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);
				newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		else {
			logger.info("Configuration comes not null");
		}
		return newAPIJobConfiguration.getConfiguration();
	}
}