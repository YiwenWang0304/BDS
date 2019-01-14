package org.diku.dms.bds_project.streaming;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Interval;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.diku.dms.bds_project.SharedJavaSparkContextLocal;

import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

public class EventDStream<T> extends DStream<T> {
	/*
	 * assume a maximum delay between the producer and consumer as 1 second
	 * by setting the batch duration as 1s, you can assume that the data with event timestamp
	 * x can be located in batches x and x+1. If there is data violates this assumption, they
	 * can be ignored
	*/
	
	private final DStream<Tuple2<Time, T>> parent;
	Duration eventDelay, slideDuration;
	/**
	 * 
	 * @param parent the DStream this EventDStream depends on
	 * @param dstream the DStream `parent` depends on
	 * @param eventDelay the largest duration of delay between event and processing time
	 * @param slideDuration the sliding length of this EventDStream
	 */
	public EventDStream(DStream<Tuple2<Time, T>> dstream, Duration eventDelay) {
		super(dstream.ssc(), scala.reflect.ClassTag$.MODULE$.apply(Object.class));
		this.slideDuration = dstream.slideDuration();
		this.eventDelay = eventDelay;
		//implemented
		this.parent = dstream ;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.spark.streaming.dstream.DStream#compute(org.apache.spark.streaming.Time)
	 */
	@Override
	public Option<RDD<T>> compute(Time validTime) {
		// event window corresponds to current window based on processing time 
		//implemented
		Interval eventWindow = new Interval(this.parent.zeroTime().milliseconds(), this.parent.zeroTime().milliseconds()+this.slideDuration.milliseconds());	
		Time beginTime = eventWindow.beginTime();
		Time endTime = eventWindow.endTime();
		JavaRDD<Tuple2<Time, T>> parentRDD = parent.getOrCompute(validTime).get().toJavaRDD();
		//implemented
		
		RDD<T> eventRDD = parentRDD.filter(s -> beginTime.$less$eq(s._1)&&endTime.$greater$eq(s._1)).map(s -> s._2).rdd();
		return Option.apply(eventRDD);
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.spark.streaming.dstream.DStream#dependencies()
	 */
	@Override
	public List<DStream<?>> dependencies() {
		List<DStream<?>>  list = scala.collection.immutable.List$.MODULE$.empty();
		list = list.$colon$colon(parent);
		return list;
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.apache.spark.streaming.dstream.DStream#slideDuration()
	 */
	@Override
	public Duration slideDuration() {
		return slideDuration;
	}
	
	public static <T> EventDStream<T> create(JavaPairDStream<Time, T> parent, Duration delayDuration) {
		return new EventDStream<T>(parent.dstream(), delayDuration);
	}
}
