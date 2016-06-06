package com.ash.kafkastream.offset.core;

import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import scala.Tuple2;

public class TrackExcersice {
	final static Logger logger = LoggerFactory
			.getLogger(TrackExcersice.class);

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
				sparkConf, Durations.seconds(5)); // batch of 5 Seconds

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

		JavaReceiverInputDStream<String> directKafkaStream = ssc.socketTextStream("192.168.0.104", 9999,StorageLevels.MEMORY_AND_DISK_SER_2);
		
		JavaPairDStream<UUID, Tuple2<Integer, Integer>> perSessionSensorStream = directKafkaStream.mapToPair(new PairFunction<String,UUID,  Tuple2<Integer,Integer>>() {

			@Override
			public Tuple2<UUID, Tuple2<Integer, Integer>> call(String arg0) throws Exception {
					String values[]=arg0.split(",");
					Tuple2<Integer,Integer> heartBeatandCalorie=new Tuple2<Integer, Integer>(Integer.parseInt(values[1]), Integer.parseInt(values[2]));
					UUID uuid=UUID.fromString(values[0]);
					
				return new Tuple2<UUID, Tuple2<Integer, Integer>>(uuid,heartBeatandCalorie);
			}

			
			
		});
		
		/**
		 * calculate average for this batch
		 */
		
		
		/**
		 * calculate the average per session from the session 
		 */
		
		
		
		

	    final Function2<List<Tuple2<Integer,Integer>>,Optional<Tuple2<Integer,Integer>>,Optional<Tuple2<Integer,Integer>>> updateFunction =
	            new Function2<List<Tuple2<Integer,Integer>>,Optional<Tuple2<Integer,Integer>>,Optional<Tuple2<Integer,Integer>>>() {

					/**
					 * 
					 * @param tupleToBeupdated // iterate to these emit the overall value 
					 * @param state // initial state
					 * @return
					 * @throws Exception
					 */

					@Override
					public Optional<Tuple2<Integer,Integer>> call(List<Tuple2<Integer, Integer>> tupleToBeupdated,
							Optional<Tuple2<Integer, Integer>> state) throws Exception {
						Tuple2<Integer, Integer> totalHeartbeatandCalorie=state.or(new Tuple2<Integer, Integer>(0, 0));
							int sumofCalorie = 0,sumOfHeartBeat=0;
			                for (Tuple2<Integer,Integer> datapoint : tupleToBeupdated) {
			                	sumofCalorie=sumofCalorie+datapoint._2;
			                	sumOfHeartBeat=sumOfHeartBeat+datapoint._1;
			                	
			                }
			                if(sumofCalorie!=-1 || sumOfHeartBeat!=-1)
							return Optional.of(new Tuple2<Integer,Integer>(totalHeartbeatandCalorie._1+sumOfHeartBeat,totalHeartbeatandCalorie._2+sumofCalorie));
			                else
			                	return Optional.of(new Tuple2<Integer,Integer>(0,0));	
					}
	    	
	    	
	    };
		JavaPairDStream<UUID, Tuple2<Integer, Integer>> updatedStream = perSessionSensorStream.updateStateByKey(updateFunction);
//		updatedStream.checkpoint(new Duration(20000));
		updatedStream.print();
		
	/**
	 * once the End session UUID is encounterd clear the updatesStream
	 */
		
			
		
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
