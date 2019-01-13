package org.diku.dms.bds_project.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.diku.dms.bds_project.VertexId;

/**
 * 
 * `PatternGraph` represents a pattern graph, which consists a set of query edges and a set of query vertices.
 * 
 * @param <VD> vertex attribute data type
 * @param <ED> edge attribute data type
 */
public class PatternGraph<VD, ED> {
	List<QueryVertex> vertices;
	Map<VertexId, Integer> index;
	List<QueryEdge> edges;
	/**
	 * Constructor method.
	 * 
	 * @param vertices a list of query vertices
	 * @param edges a list of query edges
	 */
	public PatternGraph(List<QueryVertex> vertices, List<QueryEdge> edges) {
		this.vertices = vertices;
		this.edges = edges;
		index = new HashMap<VertexId, Integer>();
		for (int i = 0; i < vertices.size(); ++i) {
			index.put(vertices.get(i).id, i);
		} // building up index for vertices
	}
	
	/**
	 * Attracts all edge patterns from a pattern graph.
	 * @return an array of edge patterns
	 */
	public EdgePattern[] toEdgePatterns() {
		EdgePattern[] edgePatterns = new EdgePattern[edges.size()]; 
		Object[] objs = edges.stream().map(edge->new EdgePattern(vertices.get(index.get(edge.srcId)), vertices.get(index.get(edge.dstId)), edge.attr)).toArray();
		for (int i = 0; i < objs.length; ++i) {
			edgePatterns[i] = (EdgePattern) objs[i];
		}
		return edgePatterns;
	}
}
