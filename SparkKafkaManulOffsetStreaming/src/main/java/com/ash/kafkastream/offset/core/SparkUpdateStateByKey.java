package com.ash.kafkastream.offset.core;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SparkUpdateStateByKey {
	final static Logger logger = LoggerFactory
			.getLogger(SparkUpdateStateByKey.class);

	private static JavaStreamingContext createContext(
			
			String checkpointDirectory, String appName) {
		// If you do not see this printed, that means the
		// StreamingContext has been loaded
		// from the new checkpoint
		System.out.println("Creating new context");
		logger.info("Creating new context");
		// System.out.println("arg1 "+kafkaTopicArray.toString());

		SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(
				"local[2]");
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

		JavaReceiverInputDStream<String> directKafkaStream = ssc.socketTextStream("192.168.0.104", 9999);

		
	
			
		// get all the tweets as json
		JavaDStream<String> jsonDStream=directKafkaStream.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String arg0) throws Exception {
					
				return Arrays.asList(arg0.split(" "));
				// TODO Auto-generated method stub
				
			}
			
		});
		
	
		JavaPairDStream<String, Integer> pairedRdd = jsonDStream.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(arg0,1);
			}
			
		});
		
		
		JavaPairDStream<String, Integer> batchRddWordCount = pairedRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});
		
		
	    
	    final Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
	            new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
	              @Override
	              public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
	                Integer newSum = state.or(0);
	                for (Integer value : values) {
	                  newSum += value;
	                }
	                return Optional.of(newSum);
	              }
	            };
	    
		
	            
	   /*         JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
	            	      new Function2<Integer, Integer, Integer>() {
	            	        public Integer call(Integer i1, Integer i2) { return i1 + i2; }
	            	      },
	            	      new Function2<Integer, Integer, Integer>() {
	            	        public Integer call(Integer i1, Integer i2) { return i1 - i2; }
	            	      },
	            	      new Duration(60 * 5 * 1000),
	            	      new Duration(1 * 1000)
	            	    );
	            */
	            JavaPairDStream<String, Integer> stateDstream = batchRddWordCount.updateStateByKey(updateFunction);
	            
	            JavaPairDStream<Integer, String> swappedPair = stateDstream.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	                @Override
	                public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	                    return item.swap();
	                }

	             });
	            
	            swappedPair= swappedPair.transformToPair(new Function<JavaPairRDD<Integer,String>, JavaPairRDD<Integer,String>>() {

					@Override
					public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0.sortByKey(false);
					}
	            	
				});
	            
		
	            swappedPair.foreach(new Function<JavaPairRDD<Integer,String>, Void>() {
					
					@Override
					public Void call(JavaPairRDD<Integer, String> arg0) throws Exception {
						arg0.foreach(new VoidFunction<Tuple2<Integer,String>>() {
							
							@Override
							public void call(Tuple2<Integer,String > arg0) throws Exception {								// TODO Auto-generated method stub
								System.out.println();
								System.out.println("The update State "+arg0._1+" ***** " +arg0._2);
							}
						});
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

	

		final String appName="updatePOC";
		final String checkpointDirectory="D:\\SparkCheckPoint";

		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return createContext(checkpointDirectory, appName);
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
