package com.ash.kafkastream.offset.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class TestStreaming {
	/**
	 * Pass Kafka Topic name ,IP address as command line argument args[1] =kafka
	 * topic // comma seperated args[2] =kafka broker'
	 * 
	 * @param args
	 */
	final static Logger logger = LoggerFactory.getLogger(TestStreaming.class);

	public static void main(String[] args) {

		if (args.length != 3) {
			logger.error("Number of arguments passed are less that what is required ,Number of argunments passed is "
					+ args.length);
			System.exit(1);
		}
		final String kafkaTopicArray[] = args[0].split(",");
		final String appName = args[2];
		final Set<String> kafkaTopicSet = new HashSet<String>(Arrays.asList(kafkaTopicArray));
		final String kafkaBrokers = args[1];
		final String checkpointDirectory = "D:\\sparkoffsetcheckpoint";
		// final String appName = "Kustomer360TwitterPipeline";
		final Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", kafkaBrokers);
		kafkaParams.put("auto.offset.reset", "smallest");//
		kafkaParams.put("group", "g13"); //

		// Function to create JavaStreamingContext without any output operations
		// (used to detect the new context)
		Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
			/**
			 * Default Serialization ID
			 */
			private static final long serialVersionUID = 1L;

			public JavaStreamingContext call() {
				return createContext(kafkaTopicSet, kafkaParams, checkpointDirectory, appName);
			}

			private JavaStreamingContext createContext(Set<String> kafkaTopic, Map<String, String> kafkaParams,
					String checkpointDirectory, String appName) {

				// If you do not see this printed, that means the
				// StreamingContext has been loaded
				// from the new checkpoint
				logger.info("Creating new context");
				//spark conf
				SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local[2]");
				// .set("spark.driver.allowMultipleContexts", "true");
				// ;
				// Create the context with a 1 second batch size

				JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
				try {
					ssc.checkpoint(checkpointDirectory);

					JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
							String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
							kafkaTopicSet);
					directKafkaStream.print();
					
				// Hold a reference to the current offset ranges, so it can be used downstream
					 final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
						
					 directKafkaStream.transformToPair(
					   new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
					     @Override
					     public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
					       OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
					       offsetRanges.set(offsets);
					       return rdd;
					     }
					   }
					 ).foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

						@Override
						public void call(JavaPairRDD<String, String> rdd) throws Exception {
							for (OffsetRange o : offsetRanges.get()) {
								System.out.println("*************************");
						         System.out.println(
						           o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
						         );
						       }
							
						}
						 
					});
					 

					 
					 /*						 directKafkaStream.map(new Function<Tuple2<String,String>, String>() {

						@Override
						public String call(Tuple2<String, String> arg0) throws Exception {
							// TODO Auto-generated method stub
							return arg0._2;
						}
						 
					}).foreachRDD(new VoidFunction<JavaRDD<String>>() {

						@Override
						public void call(JavaRDD<String> arg0) throws Exception {
							arg0.foreach(new VoidFunction<String>() {

								@Override
								public void call(String arg0) throws Exception {
									System.out.println(arg0);
									
								}
								
							});
							
						}
						
					});*/
					 
					 directKafkaStream.print();
					 
					
			
					} catch (Exception e) {
						e.printStackTrace();
					}
				

				return ssc;
			}
		};

		/**
		 * Create a new context or gets from the checkpoint directory and awaits
		 * termination
		 */
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
		ssc.start();
		ssc.awaitTermination();
	}

}
