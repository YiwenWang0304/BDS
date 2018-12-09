package org.diku.yuan.bds_project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.Edge;
import org.diku.dms.bds_project.EdgeRDD;
import org.diku.dms.bds_project.VertexId;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

@SuppressWarnings("serial")
public class EdgeRDDTest extends SharedJavaSparkContextLocal implements Serializable {
	
	public List<Edge<Integer>> sampleEdges() {
		return Arrays.asList(
				new Edge<Integer>(new VertexId(2L), new VertexId(4L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(2L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(4L), 1),
				new Edge<Integer>(new VertexId(3L), new VertexId(5L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(3L), 1)
			);
	}
	
	@Test
	public void testEdge() {
		List<Edge<Integer>> testEdges = sampleEdges();
		List<Edge<Integer>> sortedEdges = new ArrayList<Edge<Integer>>();
		for (Edge<Integer> edge : testEdges) {
			sortedEdges.add(edge);
		}
		Collections.sort(sortedEdges, Edge.comparator);
		assert(sortedEdges.get(0).compareTo(testEdges.get(1)) == 0);
		assert(sortedEdges.get(1).compareTo(testEdges.get(4)) == 0);
		assert(sortedEdges.get(2).compareTo(testEdges.get(2)) == 0);
		assert(sortedEdges.get(3).compareTo(testEdges.get(0)) == 0);
		assert(sortedEdges.get(4).compareTo(testEdges.get(3)) == 0);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testEdgeRDD() {
		List<Edge<Integer>> testEdges = sampleEdges();
		JavaRDD<Edge<Integer>> edges = jsc().parallelize(testEdges, 2);
		EdgeRDD<Integer> edgeRDD = EdgeRDD.fromEdges(edges);
		List<Edge<Integer>> collectedEdges = Arrays.asList((Edge<Integer>[]) edgeRDD.collect());
		Collections.sort(collectedEdges, Edge.comparator);
		assert(collectedEdges.get(0).compareTo(testEdges.get(1)) == 0);
		assert(collectedEdges.get(1).compareTo(testEdges.get(4)) == 0);
		assert(collectedEdges.get(2).compareTo(testEdges.get(2)) == 0);
		assert(collectedEdges.get(3).compareTo(testEdges.get(0)) == 0);
		assert(collectedEdges.get(4).compareTo(testEdges.get(3)) == 0);
	}
	
}
