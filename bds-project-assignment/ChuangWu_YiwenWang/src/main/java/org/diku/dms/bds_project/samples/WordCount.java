package org.diku.dms.bds_project.samples;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import org.apache.spark.SparkConf;

public final class WordCount {
	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("input.txt");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("[\\p{Punct}\\s]+")).iterator())
				.map(word -> word.toLowerCase()).filter(word -> word.length() > 4);
		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((c1, c2) -> c1 + c2);
		int numberOfRecords = 1000;
		JavaPairRDD<String, Integer> top1000 = counts.keyBy(tuple -> tuple._2).sortByKey(false)
				.mapToPair(tuple -> tuple._2).zipWithIndex().filter(tuple -> tuple._2 < numberOfRecords)
				.mapToPair(tuple -> tuple._1);
		JavaPairRDD<Object, Iterable<Tuple2<String, Integer>>> groupedTop1000 = top1000
				.groupBy(tuple -> tuple._1.charAt(0));
		groupedTop1000.saveAsTextFile("output");
		sc.stop();
	}
}
