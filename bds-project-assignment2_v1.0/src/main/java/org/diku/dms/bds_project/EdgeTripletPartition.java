package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.diku.dms.bds_project.query.EdgePattern;

import scala.Tuple2;

/**
 * EdgeTripletPartition stores a collections of EdgeTriplet.
 *
 * @param <ED> the edge attribute data type
 * @param <VD> the vertex attribute data type
 */
public class EdgeTripletPartition<ED, VD> extends EdgePartition<ED> implements Serializable{
	private VD[] vertexAttrs;
	/**
	 * 
	 * @param localSrcIds the local source vertex id of each edge as an index into `local2global` and `vertexAttr`
	 * @param localDstIds the local destination vertex id of each edge as an index into `local2global` and `vertexAttr`
	 * @param data the attribute associated with each edge
	 * @param index the clustered index on source vertex id as a map from each global source vertex id to the offset in the edge arrays where the cluster for that vertex id begins 
	 * @param global2local a mapping from global vertex id to referenced vertex id
	 * @param local2global a mapping from local vertex to global vertex id
	 * @param vertexAttrs attribute of vertices stored in this `EdgePartition`
	 */
	public EdgeTripletPartition(int[] localSrcIds, int[] localDstIds, ED[] data, Map<VertexId, Integer> index,
			Map<VertexId, Integer> global2local, VertexId[] local2global, VD[] vertexAttrs) {
		super(localSrcIds, localDstIds, data, index, global2local, local2global);
		this.vertexAttrs = vertexAttrs;
	}
	
	/**
	 * Given a edge pattern, return an iterator of vertex id pairs, each having the corresponding edge triplets can match this edge pattern. 
	 * 
	 * @param edgePattern the edge pattern that edge triplets contained in `EdgeTripletPartition` try to match
	 * @return the iterator of vertex id pairs, which represents the edge triplet connected by these two vertices can match   
	 */
	public Iterator<Tuple2<VertexId, VertexId>> matchEdgePattern(EdgePattern edgePattern) {
		// Please implement this function which returns source vertex and destination vertex ids of edges that match the `edgePattern`.
		return null;
	}
	
	/**
	 * Given a collection of edges (without vertex attributes) and a collection of vertices (with vertex attributes), merge them into a new `EdgeTripletPartition` which contains both vertex attributes and edge attributes.
	 * 
	 * @param <ED> edge attribute data type
	 * @param <VD> vertex attribute data type
	 * @param edgePartition an edge partition consists of edges without corresponding vertex attributes
	 * @param vertices an iterator of vertices (with vertex attributes) contained in `EdgeParition`
	 * @return a new `EdgeTripletParition`
	 */
	public static <ED, VD> EdgeTripletPartition<ED, VD> fromEdgePartitionAndVertices(EdgePartition<ED> edgePartition,
			scala.collection.Iterator<Tuple2<VertexId, VD>> vertices) {
		// Please implement this function which combines two partitions - an EdgePartition instance and a list of vertices to an EdgeTripletPartition instance. 
		return null;
	}
}
