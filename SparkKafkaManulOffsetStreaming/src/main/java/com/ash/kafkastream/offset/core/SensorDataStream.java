package com.ash.kafkastream.offset.core;

import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
import scala.Tuple3;

public class SensorDataStream {
	final static Logger logger = LoggerFactory
			.getLogger(SensorDataStream.class);

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
		
		JavaPairDStream<UUID, Tuple3<Integer,Integer, Integer>> perSessionSensorStream = directKafkaStream.mapToPair(new PairFunction<String,UUID,  Tuple3<Integer,Integer,Integer>>() {

			@Override
			public Tuple2<UUID, Tuple3<Integer, Integer,Integer>> call(String arg0) throws Exception {
					String values[]=arg0.split(",");
					Tuple3<Integer,Integer,Integer> heartBeatandCalorie=new Tuple3<Integer, Integer,Integer>(Integer.parseInt(values[1]), Integer.parseInt(values[2]),1);
					UUID uuid=UUID.fromString(values[0]);
					
				return new Tuple2<UUID, Tuple3<Integer, Integer,Integer>>(uuid,heartBeatandCalorie);
			}

			
			
		});
		
		/**
		 * calculate average for this batch
		 */
		
		
		/**
		 * calculate the average per session from the session 
		 */
		
		
		
		

	    final Function2<List<Tuple3<Integer,Integer,Integer>>,Optional<Tuple3<Integer,Integer,Integer>>,Optional<Tuple3<Integer,Integer,Integer>>> updateFunction =
	            new Function2<List<Tuple3<Integer,Integer,Integer>>,Optional<Tuple3<Integer,Integer,Integer>>,Optional<Tuple3<Integer,Integer,Integer>>>() {

					/**
					 * 
					 * @param tupleToBeupdated // iterate to these emit the overall value 
					 * @param state // initial state
					 * @return
					 * @throws Exception
					 */

					@Override
					public Optional<Tuple3<Integer,Integer,Integer>> call(List<Tuple3<Integer, Integer,Integer>> tupleToBeupdated,
							Optional<Tuple3<Integer, Integer,Integer>> state) throws Exception {
						Tuple3<Integer, Integer,Integer> totalHeartbeatandCalorie=state.or(new Tuple3<Integer, Integer,Integer>(0, 0,0));
							int sumofCalorie = 0,sumOfHeartBeat=0,items=0;
			                for (Tuple3<Integer,Integer,Integer> datapoint : tupleToBeupdated) {
			                	sumofCalorie=sumofCalorie+datapoint._2();
			                	sumOfHeartBeat=sumOfHeartBeat+datapoint._1();
			                	items=items+datapoint._3();
			                	
			                }
			                if(sumofCalorie!=-1 || sumOfHeartBeat!=-1)
							return Optional.of(new Tuple3<Integer,Integer,Integer>(totalHeartbeatandCalorie._1()+sumOfHeartBeat,totalHeartbeatandCalorie._2()+sumofCalorie,
									totalHeartbeatandCalorie._3()+items));
			                else
			                	return Optional.of(new Tuple3<Integer,Integer,Integer>(0,0,1));	
					}
	    	
	    	
	    };
		JavaPairDStream<UUID, Tuple3<Integer, Integer,Integer>> updatedStream = perSessionSensorStream.updateStateByKey(updateFunction);
		JavaPairDStream<UUID, Tuple2<Integer, Integer>> avgStream = updatedStream.transformToPair(new Function<JavaPairRDD<UUID,Tuple3<Integer,Integer,Integer>>, JavaPairRDD<UUID, Tuple2<Integer, Integer>>>() {

			

			@Override
			public JavaPairRDD<UUID, Tuple2<Integer, Integer>> call(
					JavaPairRDD<UUID, Tuple3<Integer, Integer, Integer>> rdd) throws Exception {
				return 	rdd.mapToPair(new PairFunction<Tuple2<UUID,Tuple3<Integer,Integer,Integer>>, UUID, Tuple2<Integer,Integer>>() {

						@Override
						public Tuple2<UUID, Tuple2<Integer, Integer>> call(
								Tuple2<UUID, Tuple3<Integer, Integer, Integer>> t) throws Exception {
							// TODO Auto-generated method stub
							Integer avgheartbeat=(t._2()._1()/t._2()._3());
							Integer avgcalorie=(t._2()._2()/t._2()._3());
							System.out.println("Tital calorie"+t._2()._2());
							System.out.println("Tital heartBEat"+t._2()._1());
							System.out.println("Tital count"+t._2()._3());
							Tuple2<Integer,Integer>  calandHeartBeatAvg=new Tuple2<Integer,Integer>(avgheartbeat,avgcalorie);
							return new Tuple2<UUID,Tuple2<Integer,Integer>>(t._1(),calandHeartBeatAvg);
						}
						
					});
				
				
			}
		});
		updatedStream.checkpoint(new Duration(20000));
		avgStream.print();
		
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
