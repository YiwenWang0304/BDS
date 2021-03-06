package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import scala.Tuple2;

/**
 * ShippableVertexPartition stores vertices of a partition with `routingTable`. 
 */
@SuppressWarnings("serial")
public class ShippableVertexPartition<VD> extends VertexPartition<VD> implements Serializable {

	public RoutingTablePartition routingTable;

	public ShippableVertexPartition(VertexIdToIndexMap index, VD[] data, RoutingTablePartition routingTable) {
		super(index, data);
		this.routingTable = routingTable;
	}
	
	/**
	 * Given vertices contained in one partition with the corresponding routing table, create the new `ShippableVertexPartition`.
	 * 
	 * @param <VD> the vertex attribute data type
	 * @param vertexIter an iterator of vertices
	 * @param routingTable a `RoutingTablePartition` instance
	 * @return a new `VertexPartition` as an iterator
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	// Given a collection of vertices, build up the VertexPartition instance.
	public static <VD> Iterator<VertexPartition> fromVerticesAndRoutingTable(Iterator<Tuple2<VertexId, VD>> vertexIter,
			RoutingTablePartition routingTable) { 
		VertexIdToIndexMap map = new VertexIdToIndexMap();
		int current = 0;
		List<VD> data = new ArrayList<VD>();
		
		while (vertexIter.hasNext()) {
			Tuple2<VertexId, VD> tuple = vertexIter.next();
			
			// add each vertex id to the map.
			if (!map.containsKey(tuple._1)) { 
				try {
					map.put(tuple._1, current++);
				} catch (Exception e) {
					e.printStackTrace();
				}
				data.add(tuple._2);
			}
			
		}
		
		return Arrays.asList(
				new VertexPartition[] { new ShippableVertexPartition(map, (VD[]) data.toArray(), routingTable) }) // return the new ShippableVertexPartition instance as an iterator
				.iterator();
	}

}
