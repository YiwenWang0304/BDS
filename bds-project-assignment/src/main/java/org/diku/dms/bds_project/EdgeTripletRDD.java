package org.diku.dms.bds_project;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
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

@SuppressWarnings("serial")
public class EdgeTripletRDD<ED, VD> extends RDD<EdgeTriplet<ED, VD>> {
	public final JavaRDD<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> partitionsRDD;
	
	/**
	 * Constructor function, please change the input and implement it.
	 */
	public EdgeTripletRDD(JavaRDD<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> partitionsRDD) {
		//implemented
		super(partitionsRDD.rdd(), scala.reflect.ClassTag$.MODULE$.apply(EdgeTriplet.class));
		this.partitionsRDD=partitionsRDD;
	}
	
	/**
	 * Given an edge pattern, find out all of its matches and return them as a JavaRDD. 
	 * 
	 * @param edgePattern the edge pattern that edge triplets contained in `EdgeTripletRDD` try to match
	 * @return an JavaRDD consisting of a collection of matches
	 */
	@SuppressWarnings({ "rawtypes", "null" })
	public JavaRDD<Match> matchEdgePattern(EdgePattern edgePattern) {
		//implemented
		List<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetripletpartitions=partitionsRDD.collect();
		Iterator<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetripletpartitionsItr=edgetripletpartitions.iterator();
		
		Iterator<Tuple2<VertexId, VertexId>> matchedRDDItr = null;
		List<Match> matches=new ArrayList<Match>(null);
		
		while(edgetripletpartitionsItr.hasNext()) {
			EdgeTripletPartition<ED,VD> edgetripletpartition=edgetripletpartitionsItr.next()._2;
			matchedRDDItr= edgetripletpartition.matchEdgePattern(edgePattern);
			while(matchedRDDItr.hasNext()) {
				Match match = null;
				match.vertexs.add(matchedRDDItr.next()._1);
				match.vertexs.add(matchedRDDItr.next()._2);
				matches.add(match);
			}
		}		

		return SharedJavaSparkContextLocal.jsc().parallelize(matches);
	}
	
	public static <ED,VD> EdgeTripletRDD<ED, VD> fromEdgeTripletPartitions(
			JavaRDD<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetripletPartitions) {
		return new EdgeTripletRDD<ED, VD> (edgetripletPartitions);
	}


	@SuppressWarnings("unchecked")
	@Override
	public scala.collection.Iterator<EdgeTriplet<ED, VD>> compute(Partition part, TaskContext context) {
		//implemented
		Tuple2<PartitionId, EdgeTripletPartition<ED,VD>> tuple=(Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>) this
				.firstParent(scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)).iterator(part, context)
				.next();
		return tuple._2.iterator();
		
	}

	@Override
	public Partition[] getPartitions() {
		//implemented
		return this.firstParent(scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)).partitions();
	}

	//implemented
	public static <ED, VD > EdgeTripletRDD<ED, VD> fromEdgeTriplets(JavaRDD<EdgeTriplet <ED, VD >> edgetriplets) {
		Function2<Integer, Iterator<EdgeTriplet<ED, VD>>, Iterator<Tuple2<PartitionId, EdgeTripletPartition<ED, VD>>>> f = new 
				Function2<Integer, Iterator<EdgeTriplet<ED,VD>>, Iterator<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>>>() {
			@Override
			public Iterator<Tuple2<PartitionId, EdgeTripletPartition<ED, VD>>> call(Integer pid, Iterator<EdgeTriplet<ED,VD>> edgetriplets)
					throws Exception {
				EdgeTripletPartitionBuilder<ED,VD> builder = new EdgeTripletPartitionBuilder<ED,VD>();
				while (edgetriplets.hasNext()) {
					EdgeTriplet<ED,VD> e = edgetriplets.next();
					builder.add(e.srcId,e.srcAttr, e.dstId,e.dstAttr, e.attr);
				}
				Tuple2<PartitionId, EdgeTripletPartition<ED, VD>> tuple = new 
						Tuple2<PartitionId, EdgeTripletPartition<ED, VD>>(new PartitionId(pid), builder.toEdgeTripletPartition());
				return  Arrays.asList(tuple).iterator();
			}
		};
		JavaRDD<Tuple2<PartitionId, EdgeTripletPartition<ED,VD>>> edgetripletPartitions = edgetriplets.mapPartitionsWithIndex(f, true);
		return EdgeTripletRDD.fromEdgeTripletPartitions(edgetripletPartitions);
	}

}
