package org.diku.dms.bds_project;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.diku.dms.bds_project.query.EdgePattern;
import org.diku.dms.bds_project.query.MatchesRDD;
import org.diku.dms.bds_project.query.PatternGraph;

import scala.Tuple2;

/**
 * Graph consists of three RDDs: `VertexRDD`, `EdgeRDD` and `EdgeTripletRDD`.
 * 
 * `vertices` contains a collection of vertices.
 * `edges` contains a collection of edges.
 * `edgeTriplets` is something that combines `vertices` and `edges`, which has vertex attributes on edges.
 *
 * @param <VD> vertex attribute data type
 * @param <ED> edge attribute data type
 */
public class Graph<VD, ED> implements Serializable {
	public VertexRDD<VD> vertices;
	public EdgeRDD<ED> edges;
	public EdgeTripletRDD<ED, VD> edgeTriplets;
	
	/**
	 * 
	 * @param vertices a collection of vertices
	 * @param edges a collection of edges
	 */
	public Graph(VertexRDD<VD> vertices, EdgeRDD<ED> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}

	public static <VD, ED> Graph<VD, ED> fromEdgesAndVetices(JavaRDD<Edge<ED>> edges,
			JavaPairRDD<VertexId, VD> vertices) {
		EdgeRDD<ED> edgeRDD = EdgeRDD.fromEdges(edges);
		VertexRDD<VD> vertexRDD = VertexRDD.fromVerticesAndEdgeRDD(vertices, edgeRDD);
		return new Graph(vertexRDD, edgeRDD);
	}
	
	/**
	 * The example of how to count the number of edges contained in the graph.
	 * see EdgeRDD.numEdges() and EdgePartition.numEdges()
	 * @return the number of edges contained in this graph
	 */
	public long numEdges() { 
		return edges.numEdges();
	}

	/**
	 * The example of how to count the number of vertices contained in the graph.
	 * see VertexRDD.numVertices() and VertexPartition.numVertices()
	 * 
	 * @return the number of vertices contained in this graph
	 */
	public long numVertices() {  
		return vertices.numVertices();
	}
	
	/**
	 * Ship vertex attributes from `vertices` (according to `routingTable` in each `VertexPartition`) to `edges`, obtaining the new `EdgeTripletRDD`. 
	 */
	public void shipVertexAttributes() {
		// Please implement this function
	}
	
	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represent a vertex id with indegree of the vertex 
	 */
	public JavaRDD<Tuple2<VertexId, Long>> inDegrees() {
		return edges.inDegrees();
	}
	
	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represent a vertex id with outdegree of the vertex 
	 */
	public JavaRDD<Tuple2<VertexId, Long>> outDegrees() {
		return edges.outDegrees();
	}

	/**
	 * 
	 * @return the `JavaRDD`, each element of which is a tuple which represent a vertex id with total degree of the vertex 
	 */
	public JavaRDD<Tuple2<VertexId, Long>> degrees() {
		return edges.degrees();
	}
	
	/**
	 * Given a edge pattern, return `MatchesRDD` which contains a collection of vertex id pairs, which has the edge triplet can match this edge pattern.
	 * 
	 * @param edgePattern the edge pattern that edge triplets contained in this graph try to match
	 * @return a new `MatchesRDD`
	 */
	public MatchesRDD matchEdgePattern(EdgePattern edgePattern) {
		return new MatchesRDD(edgeTriplets.matchEdgePattern(edgePattern));
	}
	
	/**
	 * Given a pattern graph, return the matches of subgraph contained in this graph as `MatchesRDD`.
	 * 
	 * @param patternGraph
	 * @return a new `MatchesRDD`
	 */
	public MatchesRDD match(PatternGraph patternGraph) {
		// Please implement this function.
		return null;
	}
}
