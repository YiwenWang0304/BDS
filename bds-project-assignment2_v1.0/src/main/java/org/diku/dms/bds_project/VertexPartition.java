package org.diku.dms.bds_project;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.util.collection.BitSet;

import scala.Tuple2;

/*
 * 
 * The iterator() function returns an iterator of Vertex<VD> by reading data from these data structures.
 */

/**
 * A `VertexPartition` stores a collection of vertices.
 * 
 * @param <VD> vertex attribute data type
 */
public class VertexPartition<VD> implements Serializable {
	
	VertexIdToIndexMap index = null;
	VD[] data = null;
	int size;
	
	/**
	 * 
	 * @param index an bijection map structure 
	 * @param data an array of attributes of vertices stored in `VertexPartition`
	 */
	public VertexPartition(VertexIdToIndexMap index, VD[] data) {
		this.index = index;
		this.data = data;
		this.size = data.length;
	}
	
	/**
	 *  
	 * @return the number of vertices contained in `VertexPartition`
	 */
	public int numVertices() {
		return size;
	}

	/**
	 * Given a vertex id, return its vertex attribute.
	 * 
	 * @param vid vertex id of an vertex contained in `VertexPartition`
	 * @return the attribute of vertex referenced by `vid`
	 */
	public VD apply(VertexId vid) { // get the vertex label of vertex 
		return data[index.get(vid)];
	}
	
	/**
	 * 
	 * @return an iterator of tuples which contain vertex id with its vertex attribute stored in `VertexPartition`.
	 */
	public scala.collection.Iterator<Tuple2<VertexId, VD>> iterator() {
		return new scala.collection.AbstractIterator<Tuple2<VertexId,VD>>() { // return a new iterator of vertices contained in this partition
			private int pos = 0;
			
			@Override
			public boolean hasNext() {
				return pos < VertexPartition.this.data.length;
			}

			@Override
			public Tuple2<VertexId, VD> next() {
				Tuple2<VertexId, VD> tuple = new Tuple2(VertexPartition.this.index.getValue(pos), data[pos]);
				pos++;
				return tuple;
			}};
	}
}
