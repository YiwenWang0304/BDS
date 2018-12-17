package org.diku.dms.bds_project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

//implementing
public class EdgeTripletPartitionBuilder<ED, VD> {

	private List<EdgeTriplet<ED,VD>> edgetriplets;
	
	public EdgeTripletPartitionBuilder() {
		edgetriplets = new ArrayList<EdgeTriplet<ED,VD>>();
	}
	
	public void add(VertexId srcId, VD srcAttr, VertexId dstId, VD dstAttr, ED attr) {
		edgetriplets.add(new EdgeTriplet<ED,VD>(new Tuple2<VertexId, VD>(srcId,srcAttr), new Tuple2<VertexId, VD>(dstId,dstAttr), attr));
	}
	
	/**
	 * Convert a collection of edgetriplets to a new `EdgeTripletPartition`.
	 * 
	 * @return a new `EdgeTripletPartition`
	 */
	@SuppressWarnings("unchecked")
	public EdgeTripletPartition<ED,VD> toEdgeTripletPartition() {
		Collections.sort(edgetriplets, EdgeTriplet.comparator); 

		int[] localSrcIds = new int[edgetriplets.size()];
		int[] localDstIds = new int[edgetriplets.size()];
		ED[] edgelabel = (ED[]) new Object[edgetriplets.size()];
		VD[] vertexlabel=(VD[]) new Object[edgetriplets.size()*2];
		Map<VertexId, Integer> index = new HashMap<VertexId, Integer>();
		Map<VertexId, Integer> global2local = new HashMap<VertexId, Integer>();
		List<VertexId> local2global = new ArrayList<VertexId>();
		
		if (edgetriplets.size() > 0) {
			index.put(edgetriplets.get(0).srcId, 0); // set index for the cluster of the first edge
			VertexId currSrcId = edgetriplets.get(0).srcId;
			int currLocalId = -1;
			int i = 0;
			while (i < edgetriplets.size()) {
				VertexId srcId = edgetriplets.get(i).srcId;
				VertexId dstId = edgetriplets.get(i).dstId;
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
				edgelabel[i] = edgetriplets.get(i).attr; // save the edge attribute
				vertexlabel[i] = edgetriplets.get(i).srcAttr; // save the src vertex attribute
				vertexlabel[i+1] = edgetriplets.get(i).dstAttr; // save the dst vertex attribute
				if (srcId != currSrcId) { // start a new cluster of edges
					currSrcId = srcId;
					index.put(currSrcId, i); // set index for the new cluster of edges
				}
				i++;
			}
		}
		

		return new EdgeTripletPartition<ED, VD>(localSrcIds, localDstIds, edgelabel, index, global2local,
				local2global.toArray(new VertexId[local2global.size()]),vertexlabel);
	}

	


}
