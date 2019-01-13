package org.diku.yuan.bds_project;

import static org.junit.Assert.assertTrue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.Edge;
import org.diku.dms.bds_project.Graph;
import org.diku.dms.bds_project.VertexId;
import org.junit.Test;

public class GraphTest extends SharedJavaSparkContextLocal {
	@Test
	public void testGraph() {
		JavaRDD<Edge<Integer>> edges = jsc().parallelize(EdgeRDDTest.sampleEdges(), 2);
		JavaPairRDD<VertexId, Integer> vertices = jsc().parallelize(EdgeRDDTest.sampleVertices(), 2).mapToPair(tuple->tuple);
		Graph<Integer, Integer> graph = Graph.fromEdgesAndVetices(edges, vertices);
		assertTrue(graph.numEdges() == 5L);
		assertTrue(graph.numVertices() == 5L);
	}
}
