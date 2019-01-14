package org.diku.dms.bds_project.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.diku.dms.bds_project.RoutingTablePartition;
import org.diku.dms.bds_project.ShippableVertexPartition;
import org.diku.dms.bds_project.VertexId;
import org.diku.dms.bds_project.VertexPartition;

import scala.Tuple2;
import twitter4j.TwitterException;

public class WindowingWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("bds-project-streaming").setMaster("local[4]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000)); // set the batch duration as 1s
		jssc.sparkContext().setLogLevel("ERROR");
		wordCount(jssc, Durations.seconds(5), Durations.seconds(3)); // set 5s as the window duration and 3s as the slide duration 
	}
	
	/**
	 * 
	 * @param jssc the java spark streaming context
	 * @param windowDuration the size of window
	 * @param slideDuration the slide length of window
	 */
	private static void wordCount(JavaStreamingContext jssc, Duration windowDuration, Duration slideDuration) {
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put("test", 1);
		JavaDStream<Tuple2<Long, String>> eventTextStream = jssc.receiverStream(new EventTextStreamKafkaReceiver());
		eventTextStream.map(tuple->tuple._2).window(windowDuration, slideDuration)
		.flatMap(s->Arrays.asList(s.split(" ")).iterator()).mapToPair(s->new Tuple2<String, Long>(s, 1L)).reduceByKey((a, b)->a+b).print();
		EventTextStreamKafkaProducer.startProducerForTextFile("input.txt", 100, 1000); // start the producer thread
		 jssc.start();
		 try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			;
		}
	}
	
	/**
	 * 
	 * @param jssc the java spark streaming context
	 * @param windowDuration the size of window
	 * @param slideDuration the slide length of window
	 * @throws TwitterException 
	 */
	public static void topK2Gram(JavaStreamingContext jssc, Duration windowDuration, Duration slideDuration) throws TwitterException {
		//implemented
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put("test", 1);
		JavaDStream<Tuple2<Long, String>> eventTextStream = jssc.receiverStream(new EventTextStreamKafkaReceiver());
		//eventTextStream.map(tuple->tuple._2).window(windowDuration, slideDuration).flatMap(s->Arrays.asList(s.split(" ")).iterator());
		JavaDStream<String> neweventTextStream=eventTextStream.map(tuple->tuple._2).window(windowDuration, slideDuration)
		.flatMap(new FlatMapFunction<String, String> () {
			@Override
			public Iterator<String> call(String s) throws Exception {
				Iterator<String> itr=Arrays.asList(s.split(" ")).iterator();
				String string1=null;
				String string2=null;
				List<String> newlist=new ArrayList<String>();
				while(itr.hasNext()) {
					if(string2==null) string1=itr.next();
					else string1=string2;
					string2=itr.next();
					newlist.add(string1+" "+string2);
				}
				return newlist.iterator();
			}
		}
		);	 
		
		neweventTextStream.mapToPair(s->new Tuple2<String, Long>(s, 1L)).reduceByKey((a, b)->a+b).mapToPair(Tuple2::swap).transformToPair(s->s.sortByKey(false)).print();
		String filename = "tweetstream.txt";
		EventTextStreamKafkaProducer.startProduceForTwitterData(filename, 100, 1000); // start the producer thread
		 jssc.start();
		 try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			;
		}
	}
	
	/**
	 * 
	 * @param jssc the java spark streaming context
	 * @param windowDuration the size of window
	 * @param slideDuration the slide length of window
	 * @param eventDelay the event delay respect to processing time (e.g., when set as 1s, the messages whose event time smaller than 19s should arrive as latest as 20s)
	 * @throws TwitterException 
	 */
	public static void topK2GramEventBased(JavaStreamingContext jssc, Duration windowDuration, Duration slideDuration, Duration eventDelay) throws TwitterException {
		//implemented
		// new EventDStream to convert data stream from processing time to event time???
		
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put("test", 1);
		JavaDStream<Tuple2<Long, String>> processingTextStream = jssc.receiverStream(new OrderedEventTextStreamKafkaReceiver(eventDelay));

		JavaDStream<Tuple2<Time, String>> eventTextStream = processingTextStream.map(t->new Tuple2<Time, String>(new Time(t._1), t._2));
		EventDStream<String> eventDstream  = new EventDStream<String>(eventTextStream.dstream(), eventDelay);
		
		JavaDStream<String> eventjavaDStream = new JavaDStream<String>(eventDstream, scala.reflect.ClassTag$.MODULE$.apply(String.class));
		
		eventjavaDStream.window(windowDuration, slideDuration)
		.flatMap(new FlatMapFunction<String, String> () {
			@Override
			public Iterator<String> call(String s) throws Exception {
				Iterator<String> itr=Arrays.asList(s.split(" ")).iterator();
				String string1=null;
				String string2=null;
				List<String> newlist=new ArrayList<String>();
				while(itr.hasNext()) {
					if(string2==null) string1=itr.next();
					else string1=string2;
					string2=itr.next();
					newlist.add(string1+" "+string2);
				}
				return newlist.iterator();
			}
		}).mapToPair(s->new Tuple2<String, Long>(s, 1L)).reduceByKey((a, b)->a+b).print();
		
		
		String filename = "tweetstream.txt";
		EventTextStreamKafkaProducer.startProduceForTwitterData(filename, 100, 1000); // start the producer thread
		 jssc.start();
		 try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			;
		}
	}
	
	/**
	 * 
	 * 
	 * @param jssc the java spark streaming context
	 * @param windowDuration the size of window
	 * @param slideDuration the slide length of window
	 * @throws TwitterException 
	 */
	public static void sharingTopK2Gram(JavaStreamingContext jssc, Duration windowDuration, Duration slideDuration) throws TwitterException {
		//implementing
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put("test", 1);
		JavaDStream<Tuple2<Long, String>> eventTextStream = jssc.receiverStream(new EventTextStreamKafkaReceiver());
		//eventTextStream.map(tuple->tuple._2).window(windowDuration, slideDuration).flatMap(s->Arrays.asList(s.split(" ")).iterator());
		Duration segmentSize=new Duration(windowDuration.milliseconds()-slideDuration.milliseconds());
		
		eventTextStream.map(tuple->tuple._2).window(segmentSize,segmentSize)
		.flatMap(new FlatMapFunction<String, String> () {
			@Override
			public Iterator<String> call(String s) throws Exception {
				Iterator<String> itr=Arrays.asList(s.split(" ")).iterator();
				String string1=null;
				String string2=null;
				List<String> newlist=new ArrayList<String>();
				while(itr.hasNext()) {
					if(string2==null) string1=itr.next();
					else string1=string2;
					string2=itr.next();
					newlist.add(string1+" "+string2);
				}
				return newlist.iterator();
			}
		}).mapToPair(s->new Tuple2<String, Long>(s, 1L)).reduceByKey((a, b)->a+b).mapToPair(Tuple2::swap).transformToPair(s->s.sortByKey(false)).print();
		
		String filename = "tweetstream.txt";
		EventTextStreamKafkaProducer.startProduceForTwitterData(filename, 100, 1000); // start the producer thread
		 jssc.start();
		 try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			;
		}
		
		
	}
	
	
}