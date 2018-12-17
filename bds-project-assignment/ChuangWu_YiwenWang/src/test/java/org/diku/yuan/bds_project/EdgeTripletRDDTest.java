package org.diku.yuan.bds_project;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.Edge;
import org.diku.dms.bds_project.EdgeTriplet;
import org.diku.dms.bds_project.EdgeTripletRDD;
import org.diku.dms.bds_project.SharedJavaSparkContextLocal;
import org.diku.dms.bds_project.VertexId;
import org.junit.Test;

import scala.Tuple2;

@SuppressWarnings("serial")
public class EdgeTripletRDDTest extends SharedJavaSparkContextLocal implements Serializable {

	public List<EdgeTriplet<Integer,Integer>> sampleEdgeTriplets() {
		return Arrays.asList(
				new EdgeTriplet<Integer,Integer>(new Tuple2<VertexId, Integer>(new VertexId(2L),1), new Tuple2<VertexId, Integer>(new VertexId(4L),2), 1),
				new EdgeTriplet<Integer,Integer>(new Tuple2<VertexId, Integer>(new VertexId(1L),3), new Tuple2<VertexId, Integer>(new VertexId(2L),4), 1),
				new EdgeTriplet<Integer,Integer>(new Tuple2<VertexId, Integer>(new VertexId(1L),1), new Tuple2<VertexId, Integer>(new VertexId(4L),2), 1),
				new EdgeTriplet<Integer,Integer>(new Tuple2<VertexId, Integer>(new VertexId(3L),3), new Tuple2<VertexId, Integer>(new VertexId(5L),4), 1),
				new EdgeTriplet<Integer,Integer>(new Tuple2<VertexId, Integer>(new VertexId(1L),5), new Tuple2<VertexId, Integer>(new VertexId(3L),6), 1)
			);
	}
	
	@Test
	public void testEdgeTriplet() {
		List<EdgeTriplet<Integer,Integer>> testEdgeTriplets = sampleEdgeTriplets();
		List<EdgeTriplet<Integer,Integer>> sortedEdgeTriplets = new ArrayList<EdgeTriplet<Integer,Integer>>();
		for (EdgeTriplet<Integer,Integer> edgetriplet : testEdgeTriplets) {
			sortedEdgeTriplets.add(edgetriplet);
		}
		Collections.sort(sortedEdgeTriplets, EdgeTriplet.comparator);
		assert(sortedEdgeTriplets.get(0).compareTo(testEdgeTriplets.get(1)) == 0);
		assert(sortedEdgeTriplets.get(1).compareTo(testEdgeTriplets.get(4)) == 0);
		assert(sortedEdgeTriplets.get(2).compareTo(testEdgeTriplets.get(2)) == 0);
		assert(sortedEdgeTriplets.get(3).compareTo(testEdgeTriplets.get(0)) == 0);
		assert(sortedEdgeTriplets.get(4).compareTo(testEdgeTriplets.get(3)) == 0);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testEdgeTripletRDD() {
		List<EdgeTriplet<Integer,Integer>> testEdgeTriplets = sampleEdgeTriplets();
		JavaRDD<EdgeTriplet<Integer,Integer>> edgetriplets = jsc().parallelize(testEdgeTriplets, 2);
		EdgeTripletRDD<Integer,Integer> edgeRDD = EdgeTripletRDD.fromEdgeTriplets(edgetriplets);
		List<EdgeTriplet<Integer,Integer>> collectedEdgeTriplets = Arrays.asList((EdgeTriplet<Integer,Integer>[]) edgeRDD.collect());
		Collections.sort(collectedEdgeTriplets, Edge.comparator);
		assert(collectedEdgeTriplets.get(0).compareTo(testEdgeTriplets.get(1)) == 0);
		assert(collectedEdgeTriplets.get(1).compareTo(testEdgeTriplets.get(4)) == 0);
		assert(collectedEdgeTriplets.get(2).compareTo(testEdgeTriplets.get(2)) == 0);
		assert(collectedEdgeTriplets.get(3).compareTo(testEdgeTriplets.get(0)) == 0);
		assert(collectedEdgeTriplets.get(4).compareTo(testEdgeTriplets.get(3)) == 0);
	}
}
