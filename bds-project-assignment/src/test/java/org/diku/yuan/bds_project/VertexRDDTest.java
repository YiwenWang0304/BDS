package org.diku.yuan.bds_project;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.diku.dms.bds_project.Edge;
import org.diku.dms.bds_project.EdgeRDD;
import org.diku.dms.bds_project.SharedJavaSparkContextLocal;
import org.diku.dms.bds_project.VertexId;
import org.diku.dms.bds_project.VertexRDD;
import org.junit.Test;

import scala.Tuple2;

@SuppressWarnings("serial")
public class VertexRDDTest extends SharedJavaSparkContextLocal implements Serializable {

	public List<Tuple2<VertexId, Integer>> sampleVertices() {
		return Arrays.asList(new Tuple2<VertexId, Integer>(new VertexId(1L), 1),
				new Tuple2<VertexId, Integer>(new VertexId(2L), 1), new Tuple2<VertexId, Integer>(new VertexId(3L), 1),
				new Tuple2<VertexId, Integer>(new VertexId(4L), 1), new Tuple2<VertexId, Integer>(new VertexId(5L), 1));
	}

	public List<Edge<Integer>> sampleEdges() {
		return Arrays.asList(new Edge<Integer>(new VertexId(2L), new VertexId(4L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(2L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(4L), 1),
				new Edge<Integer>(new VertexId(3L), new VertexId(5L), 1),
				new Edge<Integer>(new VertexId(1L), new VertexId(3L), 1));
	}

	@Test
	public void testVertexRDD() {
		EdgeRDD<Integer> edges = EdgeRDD.fromEdges(jsc().parallelize(sampleEdges(), 2));
		VertexRDD<Integer> vertices = VertexRDD.fromVerticesAndEdgeRDD(jsc().parallelize(sampleVertices(), 2).mapToPair(tuple->tuple), edges);
		assertTrue(vertices.count() == 5L);
	}
}
