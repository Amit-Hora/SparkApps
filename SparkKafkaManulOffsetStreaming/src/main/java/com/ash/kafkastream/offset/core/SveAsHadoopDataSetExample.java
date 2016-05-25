package com.ash.kafkastream.offset.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SveAsHadoopDataSetExample {
	final static Logger logger = LoggerFactory
			.getLogger(SveAsHadoopDataSetExample.class);

	private static JavaStreamingContext createContext(
			Set<String> kafkaTopicSet, Map<String, String> kafkaParams,
			String checkpointDirectory, String appName) {
		// If you do not see this printed, that means the
		// StreamingContext has been loaded
		// from the new checkpoint
		System.out.println("Creating new context");
		logger.info("Creating new context");
		// System.out.println("arg1 "+kafkaTopicArray.toString());

		SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(
				"local[1]");
		// Create the context with a 1 second batch size
		/* final */final JavaStreamingContext ssc = new JavaStreamingContext(
				sparkConf, Durations.seconds(10)); // i created here "final"
													// javaStreamingContext

		// for graceful shutdown of the application ...
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.warn("Shutting down streaming app...");
				ssc.stop(true, true);
				logger.warn("Shutdown of streaming app complete.");
			}
		});

		ssc.checkpoint(checkpointDirectory);

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						kafkaTopicSet);

		
	
			
		// get all the tweets as json
		JavaDStream<String> jsonDStream=directKafkaStream.map(new Function<Tuple2<String,String>, String>() {

			@Override
			public String call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._2;
			}
		});
		
		final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext());
		// Convert RDDs of the words DStream to DataFrame and run SQL query
		
		
		jsonDStream.foreachRDD(new Function<JavaRDD<String>, Void>() {

			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				
				DataFrame jsonFrame = sqlContext.jsonRDD(rdd);
				DataFrame selecteFieldFrame = jsonFrame.select("id_str","created_at","text");

				Configuration config = HBaseConfiguration.create();
				config.set("hbase.zookeeper.quorum", "d-9543");
				config.set("zookeeper.znode.parent","/hbase-unsecure");
				config.set("hbase.zookeeper.property.clientPort", "2181");
				final JobConf jobConfig=new JobConf(config,SveAsHadoopDataSetExample.class);
				
				jobConfig.setOutputFormat(TableOutputFormat.class);
				jobConfig.set(TableOutputFormat.OUTPUT_TABLE,"tableName");
				 selecteFieldFrame.javaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {

					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
						// TODO Auto-generated method stub
						return convertToPut(row);
					}
				}).saveAsHadoopDataset(jobConfig);
				

				return null;
			}
		});

		
		
		
		
		return ssc;
	}
	
	public static Tuple2<ImmutableBytesWritable, Put> convertToPut(Row row){
		String key=row.getString(0);
		Put put=new Put(Bytes.toBytes(key));
		put.addColumn(Bytes.toBytes("twt"),Bytes.toBytes("createdat"), Bytes.toBytes(row.getString(1)));
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(key)),put);
		
	};

	/**
	 * Pass Kafka Topic name ,IP address as command line argument args[1] =kafka
	 * topic // comma seperated args[2] =kafka broker'
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		/*
		 * HBaseAdmin admin; Configuration config = HBaseConfiguration.create();
		 * HTable table;
		 */

		if (args.length != 2) {
			logger.error("Number of arguments passed are less that what is required ,Number of argunments passed is "
					+ args.length);
			System.exit(1);
		}

		final String kafkaTopicArray[] = args[0].split(",");

		final Set<String> kafkaTopicSet = new HashSet<String>(
				Arrays.asList(kafkaTopicArray));
		final String kafkaBrokers = args[1];
		final String checkpointDirectory = "D:\\SparkCheckPoint\\kustomer360";
		final String appName = "Kustomer360TwitterPipeline";
		final Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		kafkaParams.put("auto.offset.reset", "smallest");//
		kafkaParams.put("group", "gtest"); //
		logger.info("arg1 " + kafkaTopicArray.toString());
		System.out.println("arg2 " + kafkaBrokers);

		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return createContext(kafkaTopicSet, kafkaParams,
						checkpointDirectory, appName);
			}
		};

		/**
		 * Create a new context or gets from the checkpoint directory and awaits
		 * termination
		 */

		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(
				checkpointDirectory, factory);
		System.out.println("Creating new context_start method is called");
		ssc.start();
		ssc.awaitTermination();
	}

}

