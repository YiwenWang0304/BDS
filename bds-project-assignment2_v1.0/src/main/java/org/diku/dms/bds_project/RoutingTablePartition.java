package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import scala.Tuple2;

/**
 * RoutingTablePartition encapsulates a routing table according to each vertex partition. 
 * For example, if an element of the routing table is a tuple of array [1, 2, 3] and PartitionId 1 means the vertices 1, 2, 3 should be routed to partition 1.
 */
public class RoutingTablePartition implements Serializable {
	public Tuple2<VertexId[], PartitionId>[] routingTable; //`routingTable` is used for routing vertex attributes from VertexRDD to EdgeRDD.

	public RoutingTablePartition(Tuple2<VertexId[], PartitionId>[] _routingTable) {
		routingTable = _routingTable;
	}
	
	/**
	 * Given partition id `pid` and edge partition `edgePartition`, generate a tuple of vertex id and partition id, for each vertex id contained in EdgePartition, and return them as an iterator. 
	 * 
	 * @param <ED> edge attribute data type
	 * @param pid partition id 
	 * @param edgePartition an `EdgePatition` instance
	 * @return an iterator of vertex id and partition id pairs
	 */
	public static <ED> Iterator<Tuple2<VertexId, PartitionId>> routingInformationFromEdgePartition(PartitionId pid,
			EdgePartition<ED> edgePartition) { // In the beginning, the information is computed from each EdgePartition instance.
		Set<VertexId> vertexIds = new HashSet<VertexId>();
		scala.collection.Iterator<Edge<ED>> iter = edgePartition.iterator();
		while (iter.hasNext()) {
			Edge<ED> edge = iter.next();
			vertexIds.add(edge.srcId);
			vertexIds.add(edge.dstId);
		} // `vertexIds` contains all vertex ids that this EdgePartition instance has.
		return vertexIds.stream().map(vid -> new Tuple2<VertexId, PartitionId>(vid, pid)).iterator(); // For each vertex id, pair up with `pid` to return tuples.
	}
	
	/**
	 * For a vertex partition, given a collection of vertex id and partition id pairs, convert them to `RoutingTablePartition.`
	 * 
	 * @param <ED> edge attribute data type
	 * @param <VD> vertex attribute data type
	 * @param numEdgePartition the number of partitions of an `EdgeRDD` instance
	 * @param iter the iterator of tuples of vertex id and partition id contained in the vertex partition
	 * @return a new `RoutingTablePartition`
	 */
	public static <ED, VD> RoutingTablePartition fromRoutingInformation(int numEdgePartition,
			Iterator<Tuple2<VertexId, PartitionId>> iter) { // Then, aggregate these information for each VertexPartition instance.
		List<VertexId>[] pid2vid = new ArrayList[numEdgePartition];
		for (int i = 0; i < numEdgePartition; ++i) {
			pid2vid[i] = new ArrayList<VertexId>();
		}
		while (iter.hasNext()) {
			Tuple2<VertexId, PartitionId> tuple = iter.next();
			pid2vid[tuple._2.id].add(tuple._1);
		} 
		Tuple2<VertexId[], PartitionId>[] routingTable = new Tuple2[numEdgePartition];
		for (int i = 0; i < numEdgePartition; ++i) {
			routingTable[i] = new Tuple2(pid2vid[i].toArray(new VertexId[pid2vid[i].size()]), new PartitionId(i));
		} // `routingTable[i]` means the an array of vertex ids should be routed to i-th partitions, PartitionId here is as same as i.
		return new RoutingTablePartition(routingTable);
	}
}
