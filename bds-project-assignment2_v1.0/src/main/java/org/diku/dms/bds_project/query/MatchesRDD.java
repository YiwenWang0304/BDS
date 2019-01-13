package org.diku.dms.bds_project.query;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import scala.reflect.ClassTag;

/**
 * A MatchesRDD instance stores a collection of matches to a partial pattern as an RDD.
 */
public class MatchesRDD extends JavaRDD<Match>{

	
	public MatchMeta meta; //Please define the `MatchMeta` class and use it here.
	  
	
	/**
	 * Constructor method.
	 * @param matches a JavaRDD consists of many `Match` instances.
	 */
	public MatchesRDD(JavaRDD<Match> matches) {
		// You may need to change the constructor function.s
		super(matches.rdd(), scala.reflect.ClassTag$.MODULE$.apply(Match.class));
	}
	
	/**
	 * The method `join()` computes the joining results of two MatchesRDD instances, result is returned as a new MatchesRDD instance.
	 * For instance, in the example of assignment description, the matches of edge pattern (u1, u2) and (u1, u3) are as follows.
	 * 
	 * |u1 u2 | |u1 u3| ----- meta data				---|
	 * |------| |-----|                             ---|
	 * |v2 v3 | |v2 v4| --|							---|-- MatchesRDD instance
	 * |v3 v4 | |v4 v5| --|-- collection of `Match` ---|
	 * |v5 v3 | |     | --|							---|
	 * 
	 * The result of join should be 
	 * |u1 u2 u3|
	 * |--------|
	 * |v2 v3 v4|
	 * |        |
	 * 
	 * @param other another `MatchesRDD` instance
	 * @return a new `MatchesRDD` according to the join
	 */
	public MatchesRDD join(MatchesRDD other) {
		
		//Please implement this function.
		 
		return null;
	}
}
