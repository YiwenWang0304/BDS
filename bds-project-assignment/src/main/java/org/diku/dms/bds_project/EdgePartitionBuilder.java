package org.diku.dms.bds_project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * EdgePartitionBuilder builds EdgePartition from a collection of edges.
 *
 * @param <ED> edge attribute data type
 */
public class EdgePartitionBuilder<ED> {
	private List<Edge<ED>> edges;
	
	public EdgePartitionBuilder() {
		edges = new ArrayList<Edge<ED>>();
	}
	
	/***
	 * Add a new edge to this `EdgePatternBuilder`
	 * 
	 * @param srcId vertex id of source vertex on the edge
	 * @param dstId vertex id of destination vertex on the edge 
	 * @param attr the attribute associated with the edge
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void add(VertexId srcId, VertexId dstId, ED attr) {
		edges.add(new Edge(srcId, dstId, attr));
	}
	
	/**
	 * Convert a collection of edges to a new `EdgePartition`.
	 * 
	 * 
	 * @return a new `EdgePartition`
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public EdgePartition<ED> toEdgePartition() {
		Collections.sort(edges, Edge.comparator); // Sort edges by the comparator

		int[] localSrcIds = new int[edges.size()];
		int[] localDstIds = new int[edges.size()];
		ED[] data = (ED[]) new Object[edges.size()];
		Map<VertexId, Integer> index = new HashMap<VertexId, Integer>();
		Map<VertexId, Integer> global2local = new HashMap<VertexId, Integer>();
		List<VertexId> local2global = new ArrayList<VertexId>();
		
		if (edges.size() > 0) {
			index.put(edges.get(0).srcId, 0); // set index for the cluster of the first edge
			VertexId currSrcId = edges.get(0).srcId;
			int currLocalId = -1;
			int i = 0;
			while (i < edges.size()) {
				VertexId srcId = edges.get(i).srcId;
				VertexId dstId = edges.get(i).dstId;
				if (!global2local.containsKey(srcId)) { 
					currLocalId += 1; // increase the number of local ids by one for a new local vertex id
					local2global.add(srcId); // make the mappings between global vertex ids and local vertex ids 
					global2local.put(srcId, currLocalId); 
				}
				localSrcIds[i] = global2local.get(srcId); // save the referenced local id as source id 
				if (!global2local.containsKey(dstId)) {
					currLocalId += 1;
					local2global.add(dstId);
					global2local.put(dstId, currLocalId);
				}
				localDstIds[i] = global2local.get(dstId); // save the referenced local id as destination id
				data[i] = edges.get(i).attr; // save the edge attribute
				if (srcId != currSrcId) { // start a new cluster of edges
					currSrcId = srcId;
					index.put(currSrcId, i); // set index for the new cluster of edges
				}

				i += 1;
			}
		}
		return new EdgePartition(localSrcIds, localDstIds, data, index, global2local,
				local2global.toArray(new VertexId[edges.size()]));
	}
}
