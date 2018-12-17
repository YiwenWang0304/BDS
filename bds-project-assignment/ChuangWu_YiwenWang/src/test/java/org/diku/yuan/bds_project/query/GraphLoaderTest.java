package org.diku.yuan.bds_project.query;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.diku.dms.bds_project.EdgeRDD;
import org.diku.dms.bds_project.Graph;
import org.diku.dms.bds_project.VertexId;
import org.diku.dms.bds_project.VertexRDD;
import org.diku.dms.bds_project.query.GraphLoader;
import org.diku.dms.bds_project.query.PatternGraph;
import org.diku.dms.bds_project.query.QueryEdge;
import org.diku.dms.bds_project.query.QueryVertex;

public class GraphLoaderTest<VD, ED> {
	
	@Test
	public void getGraphInstanceTest() throws NumberFormatException, Exception{
		GraphLoader<VD, ED> loader=new GraphLoader<VD, ED>();
		Graph<VD, ED> graph=loader.getGraphInstance("../dataset/data_forTest");
		VertexRDD<VD> vertices=graph.vertices;
		assertTrue(vertices.numVertices()==10);
		EdgeRDD<ED> edges=graph.edges;
		assertTrue(edges.numEdges()==10);
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void getPatternInstanceTest() throws IOException{
		GraphLoader<VD, ED> loader=new GraphLoader<VD, ED>();
		PatternGraph<VD, ED> pattern=loader.getPatternInstance("../dataset/pattern1");
		List<QueryVertex> vertices=pattern.vertices;
		int i=0;
		for(QueryVertex vertex:vertices) 
			assert(vertex.id.compareTo(new VertexId(i++))==0);
	
		List<QueryEdge> edges=pattern.edges;
		int j=0;
		for(QueryEdge edge:edges) {
			assert(edge.srcId.compareTo(new VertexId(j++))==0);
			assert(edge.dstId.compareTo(new VertexId(j++))==0);
		}
	}
}

