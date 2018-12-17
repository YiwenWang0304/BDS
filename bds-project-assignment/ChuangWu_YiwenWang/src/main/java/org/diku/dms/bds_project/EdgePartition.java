package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.*;

import scala.Tuple2;

/**
 * EdgePartition stores a collection of edges.
 * 
 * The edges are stored in columnar format in `localSrcIds`, `localDstIds` and
 * `data`. All vertex ids are mapped to a compact set of local vertex ids
 * according to the `global2local` map. `local2global` maps local vertex ids to
 * global vertex ids.
 * 
 * The edges are clustered by source vertex id, and the mapping from global
 * vertex id to the index of the corresponding edge cluster is stored in
 * `index`.
 * 
 * @param <ED> the edge attribute type.
 */
@SuppressWarnings("serial")
public class EdgePartition<ED> implements Serializable {
	int[] localSrcIds = null, localDstIds = null;
	ED[] data = null;
	Map<VertexId, Integer> index = null, global2local = null;
	VertexId[] local2global = null;
	int size = 0;

	/**
	 * 
	 * @param localSrcIds  the local source vertex id of each edge as an index into
	 *                     `local2global`
	 * @param localDstIds  the local destination vertex id of each edge as an index
	 *                     into `local2global`
	 * @param data         the attribute associated with each edge
	 * @param index        the clustered index on source vertex id as a map from
	 *                     each global source vertex id to the offset in the edge
	 *                     arrays where the cluster for that vertex id begins
	 * @param global2local a mapping from global vertex id to referenced vertex id
	 * @param local2global a mapping from local vertex to global vertex id
	 */
	public EdgePartition(int[] localSrcIds, int[] localDstIds, ED[] data, Map<VertexId, Integer> index,
			Map<VertexId, Integer> global2local, VertexId[] local2global) {
		this.localSrcIds = localSrcIds;
		this.localDstIds = localDstIds;
		this.data = data;
		this.index = index;
		this.global2local = global2local;
		this.local2global = local2global;
		this.size = localSrcIds.length;
	}

	/**
	 * 
	 * @return an iterator of edges contained by `EdgePartition`
	 */
	public scala.collection.Iterator<Edge<ED>> iterator() {
		return new scala.collection.AbstractIterator<Edge<ED>>() {
			private int pos = 0;

			@Override
			public boolean hasNext() {
				return pos < EdgePartition.this.size;
			}

			@Override
			public Edge<ED> next() {
				Edge<ED> edge = new Edge<ED>(local2global[localSrcIds[pos]], local2global[localDstIds[pos]], data[pos]);
				pos++;
				return edge;
			}
		};
	}

	/**
	 * 
	 * @return the number of edges contained by the `EdgePartition` instance
	 */
	public int numEdges() {
		return size;
	}

	/**
	 * 
	 * @return the iterator of tuples, which contains a vertex id with the in-degree of that vertex 
	 */
	public Iterator<Tuple2<VertexId, Long>> inDegrees() {
		//implemented
		return calDegrees(localDstIds,null);
	}

	/**
	 * 
	 * @return the iterator of tuples, which contains a vertex id with the
	 *         out-degree of that vertex
	 */
	public Iterator<Tuple2<VertexId, Long>> outDegrees() {
		//implemented
		return calDegrees(null,localSrcIds);
	}

	/**
	 * 
	 * @return the iterator of tuples, which contains a vertex id with the total
	 *         degree of that vertex
	 */
	public Iterator<Tuple2<VertexId, Long>> degrees() {
		//implemented
		return calDegrees(localDstIds,localSrcIds);
	}
	
	//implemented
	private Iterator<Tuple2<VertexId, Long>> calDegrees(int[] localdstids,int[] localsrcids) {
		List<VertexId> ids = new ArrayList<VertexId>();
		List<Integer> idlist=new ArrayList<Integer>();
			
		if(localdstids!=null) {
			idlist= Arrays.stream(localdstids).boxed().collect(Collectors.toList());
			System.out.print(localdstids.length+",");
			System.out.println(idlist.size());
			for(int i=0;i<idlist.size();i++) 
				ids.add(local2global[idlist.get(i)]);
		}else if(localsrcids!=null) {
			idlist= Arrays.stream(localsrcids).boxed().collect(Collectors.toList());
			for(int i=0;i<idlist.size();i++) 
				ids.add(local2global[idlist.get(i)]);
		}

		JavaRDD<VertexId> distData = SharedJavaSparkContextLocal.jsc().parallelize(ids);
		JavaPairRDD<VertexId, Long> pairs = distData.mapToPair(s -> new Tuple2<VertexId,Long>(s, 1L));
		JavaPairRDD<VertexId, Long> counts = pairs.reduceByKey((a, b) -> a + b);

		return counts.collect().iterator();
	}
}
