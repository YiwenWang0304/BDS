package org.diku.yuan.bds_project;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.Edge;
import org.diku.dms.bds_project.Graph;
import org.diku.dms.bds_project.VertexId;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class GraphTest extends SharedJavaSparkContextLocal {
	
	@Before
	public List<Tuple2<VertexId, Integer>> sampleVertices() {
		return Arrays.asList(new Tuple2<VertexId, Integer>(new VertexId(1L), 1),
				new Tuple2<VertexId, Integer>(new VertexId(2L), 1), new Tuple2<VertexId, Integer>(new VertexId(3L), 1),
				new Tuple2<VertexId, Integer>(new VertexId(4L), 1), new Tuple2<VertexId, Integer>(new VertexId(5L), 1));
	}
	
	@Before
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
	public void testGraph() {
		JavaRDD<Edge<Integer>> edges = jsc().parallelize(sampleEdges(), 2);
		JavaPairRDD<VertexId, Integer> vertices = jsc().parallelize(sampleVertices(), 2).mapToPair(tuple->tuple);
		Graph<Integer, Integer> graph = Graph.fromEdgesAndVetices(edges, vertices);
		assertTrue(graph.numEdges() == 5L);
		assertTrue(graph.numVertices() == 5L);
	}
}
