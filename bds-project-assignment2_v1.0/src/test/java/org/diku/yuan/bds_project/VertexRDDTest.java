package org.diku.yuan.bds_project;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.diku.dms.bds_project.EdgeRDD;
import org.diku.dms.bds_project.VertexId;
import org.diku.dms.bds_project.VertexRDD;
import org.junit.Test;

import scala.Tuple2;
import scala.collection.Iterator;

public class VertexRDDTest extends SharedJavaSparkContextLocal implements Serializable{
	@Test
	public void testVertexRDD() {
		EdgeRDD<Integer> edges = EdgeRDD.fromEdges(jsc().parallelize(EdgeRDDTest.sampleEdges(), 2));
		VertexRDD<Integer> vertices = VertexRDD.fromVerticesAndEdgeRDD(jsc().parallelize(EdgeRDDTest.sampleVertices(), 2).mapToPair(tuple->tuple), edges);
		assertTrue(vertices.count() == 5L);
	}
}
