package org.diku.yuan.bds_project.query;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.diku.dms.bds_project.VertexId;
import org.diku.dms.bds_project.query.EdgePattern;
import org.diku.dms.bds_project.query.PatternGraph;
import org.diku.dms.bds_project.query.QueryEdge;
import org.diku.dms.bds_project.query.QueryVertex;
import org.diku.dms.bds_project.query.VertexPredicate;
import org.junit.Test;

public class PatternGraphTest {
	public static List<QueryVertex> sampleQueryVertices() {
		return Arrays.asList(
				new QueryVertex<Integer>(new VertexId(1L), new VertexPredicate(VertexPredicate.Type.ATTR, 1)),
				new QueryVertex<Integer>(new VertexId(2L), new VertexPredicate(VertexPredicate.Type.ID, new VertexId(1L))),
				new QueryVertex<Integer>(new VertexId(3L), new VertexPredicate(VertexPredicate.Type.ATTR, 2))
				);
	}
	public static List<QueryEdge> sampleQueryEdges() {
		return Arrays.asList(
				new QueryEdge<Integer>(new VertexId(1L), new VertexId(2L), 0),
				new QueryEdge<Integer>(new VertexId(2L), new VertexId(3L), 1),
				new QueryEdge<Integer>(new VertexId(1L), new VertexId(3L), 2)
				);
	}
	@Test
	public void testPatternGraph() {
		PatternGraph pg = new PatternGraph(sampleQueryVertices(), sampleQueryEdges());
		assertTrue(pg.toEdgePatterns().length == 3);
	}
}
