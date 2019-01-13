package org.diku.dms.bds_project;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.diku.dms.bds_project.query.EdgePattern;
import org.diku.dms.bds_project.query.Match;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * An RDD that stores a collection of edge triplets with interface for the data type as `EdgeTriplet`.
 * The data should be essentially stored as a collection of `EdgeTripletPartition` instances. 
 *
 * @param <ED> edge attribute data type
 * @param <VD> vertex attribute data type
 */

//Remember to remove the `abstract` tag after you implement
public abstract class EdgeTripletRDD<ED, VD> extends RDD<EdgeTriplet<ED, VD>> {
	/**
	 * Constructor function, please change the input and implement it.
	 */
	public EdgeTripletRDD(RDD<?> oneParent, ClassTag<EdgeTriplet<ED, VD>> evidence$2) {
		//Please change the input and implement this function
		super(oneParent, evidence$2);
	}
	
	/**
	 * Given an edge pattern, find out all of its matches and return them as a JavaRDD. 
	 * 
	 * @param edgePattern the edge pattern that edge triplets contained in `EdgeTripletRDD` try to match
	 * @return an JavaRDD consisting of a collection of matches
	 */
	public JavaRDD<Match> matchEdgePattern(EdgePattern edgePattern) {
		//Please implement this function
		return null;
	}

}
